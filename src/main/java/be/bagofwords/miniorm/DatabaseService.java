package be.bagofwords.miniorm;

import be.bagofwords.iterator.CloseableIterator;
import be.bagofwords.logging.Log;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.LifeCycleBean;
import be.bagofwords.minidepi.annotations.Inject;
import be.bagofwords.miniorm.data.ReadField;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.lang3.StringUtils;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.*;
import java.util.stream.Stream;

import static be.bagofwords.util.Utils.noException;
import static java.util.stream.Collectors.toList;

/**
 * Created by koen on 12.11.16.
 */
public class DatabaseService implements LifeCycleBean {

    private static final int INSERT_BATCH_SIZE = 100;

    @Inject
    private ApplicationContext context;
    @Inject
    private DatabaseTypeService databaseTypeService;

    private ComboPooledDataSource pool;

    private Map<Class, InitializationMethod> cachedInitializationMethods = new HashMap<>();

    @Override
    public void startBean() {
        pool = new ComboPooledDataSource();
        String defaultProperties = "mini-orm.properties";
        String dbHost = context.getProperty("database.host", defaultProperties);
        String dbName = context.getProperty("database.name");
        String dbType = context.getProperty("database.type", defaultProperties);
        String extraArgs = context.getProperty("database.extra.args", defaultProperties);
        String jdbcUrl = "jdbc:" + dbType + "://" + dbHost + "/" + dbName ;
        if (StringUtils.isNotEmpty(extraArgs)) {
            jdbcUrl += "?" + extraArgs;
        }
        Log.i("Initiating database connection " + jdbcUrl);
        pool.setJdbcUrl(jdbcUrl);
        pool.setUser(context.getProperty("database.user"));
        pool.setPassword(context.getProperty("database.password"));
        pool.setMaxPoolSize(20);
        pool.setMinPoolSize(0);
        pool.setInitialPoolSize(5);
        pool.setAutoCommitOnClose(true);
        pool.setMaxIdleTime(20_000);
    }

    @Override
    public void stopBean() {
        pool.close();
    }

    private Connection getConnection() throws SQLException {
        Connection connection = pool.getConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    public String getType(Object object) {
        return getType(object.getClass());
    }

    public String getType(Class class_) {
        return class_.getSimpleName();
    }

    private Class[] getTypesFromArgs(Object[] args) {
        Class[] types = new Class[args.length];
        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                throw new RuntimeException("Null values are not supported for arguments");
            }
            types[i] = args[i].getClass();
        }
        return types;
    }

    public void execute(String sqlStatement) {
        execute(connection -> {
            connection.prepareStatement(sqlStatement).execute();
        });
    }

    public void execute(DatabaseAction action) {
        Connection connection = null;
        try {
            connection = getConnection();
            action.execute(connection);
            connection.commit();
        } catch (Throwable t) {
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException e) {
                    Log.e("Failed to rollback database connection!", e);
                }
            }
            throw new RuntimeException(t);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    Log.e("Failed to close database connection!", e);
                }
            }
        }
    }

    public <T> T execute(DatabaseActionWithResult<T> action) {
        return execute(action, false);
    }

    public <T> T execute(DatabaseActionWithResult<T> action, boolean keepOpen) {
        Connection connection = null;
        try {
            connection = getConnection();
            T result = action.execute(connection);
            if (!keepOpen) {
                connection.commit();
            }
            return result;
        } catch (Throwable t) {
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException e) {
                    Log.e("Failed to rollback database connection!", e);
                }
            }
            throw new RuntimeException(t);
        } finally {
            if (connection != null && !keepOpen) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    Log.e("Failed to close database connection!", e);
                }
            }
        }
    }

    public void updateObjectWithId(Object object) {
        execute(connection -> {
            List<Field> fields = getFields(object.getClass(), false).collect(toList());
            Field idField = getIdField(object.getClass());
            String query = createUpdateQuery(object, fields);
            query += "where id=?";
            PreparedStatement statement = connection.prepareStatement(query);
            fields.add(idField);
            writeFields(statement, object, fields);
            statement.executeUpdate();
        });
    }

    private String createUpdateQuery(Object object, List<Field> fields) {
        String table = getTable(object.getClass());
        String query = "update " + table + " set ";
        boolean firstField = true;
        for (Field field : fields) {
            if (!field.getName().equals("id")) {
                if (firstField) {
                    firstField = false;
                } else {
                    query += ", ";
                }
                query += escape(field.getName()) + "=? ";
            }
        }
        return query;
    }

    public void updateObject(Object object, String clause, Object... args) {
        execute(connection -> {
            updateObject(connection, object, clause, args);
        });
    }

    private void updateObject(Connection connection, Object object, String clause, Object[] args) throws SQLException, IllegalAccessException {
        List<Field> fields = getFields(object.getClass(), false).collect(toList());
        String query = createUpdateQuery(object, fields);
        query += clause;
        PreparedStatement statement = connection.prepareStatement(query);
        int ind = writeFields(statement, object, fields);
        writeFields(statement, ind, args);
        statement.executeUpdate();
    }

    public Long insertObject(Object object) {
        List<Long> ids = insertObjects(Collections.singletonList(object));
        if (ids != null && !ids.isEmpty()) {
            return ids.get(0);
        } else {
            return null;
        }
    }

    public void insertOrUpdateObject(Object object) {
        Field idField = getIdField(object.getClass());
        if (idField == null) {
            throw new RuntimeException("Object " + object + " does not have an id field. To insertOrUpdate this type of objects, you need specify a clause");
        }
        try {
            long value = idField.getLong(object);
            insertOrUpdateObject(object, "WHERE id=?", value);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Failed to get value of the id field in " + object);
        }
    }

    public void insertOrUpdateObject(Object object, String clause, Object... args) {
        execute(connection -> {
            List<?> objects = readObjects(object.getClass(), clause, args);
            if (objects.isEmpty()) {
                //insert
                insertObjects(connection, Collections.singletonList(object));
            } else if (objects.size() == 1) {
                //update
                updateObject(connection, object, clause, args);
            } else {
                //hmm that's not right
                throw new RuntimeException("The clause \"" + clause + "\" resulted in " + objects.size() + " returned objects. Expected zero or one objects");
            }
        });
    }

    public List<Long> insertObjects(List<? extends Object> objects) {
        return execute(connection -> {
            return insertObjects(connection, objects);
        });
    }

    private List<Long> insertObjects(Connection connection, List<?> objects) throws SQLException, IllegalAccessException {
        if (objects.isEmpty()) {
            return Collections.emptyList();
        }
        Class objectClass = objects.get(0).getClass();
        Field idField = getIdField(objectClass);
        boolean generateId = idField != null;
        for (Object object : objects) {
            if (object.getClass() != objectClass) {
                throw new RuntimeException("Found two types of objects " + objectClass + " and " + object.getClass());
            }
        }
        String table = getTable(objectClass);
        String query = "insert into " + table;
        List<String> fields = getFieldNames(objectClass, !generateId);
        query += " (" + getFieldsString(table, fields) + ")";
        query += " values (" + String.join(",", fields.stream().map(name -> "?").collect(toList())) + ")";
        if (generateId) {
            List<Long> ids = insertWithAutoGeneratedIds(objects, connection, query);
            assert ids.size() == objects.size();
            for (int i = 0; i < ids.size(); i++) {
                idField.set(objects.get(i), ids.get(i));
            }
            return ids;
        } else {
            insertWithoutAutoGeneratedIds(objects, connection, query);
            return null;
        }
    }

    private Field getIdField(Class objectClass) {
        try {
            Field idField = objectClass.getField("id");
            if (!idField.getType().equals(Long.class) && !idField.getType().equals(long.class)) {
                throw new RuntimeException("The id field of class " + objectClass + " is not of type Long or long");
            }
            return idField;
        } catch (NoSuchFieldException exp) {
            return null;
        }
    }

    private void insertWithoutAutoGeneratedIds(List<?> objects, Connection connection, String query) throws SQLException, IllegalAccessException {
        PreparedStatement statement = connection.prepareStatement(query);
        for (int i = 0; i < objects.size(); i++) {
            writeFields(statement, objects.get(i), true);
            statement.addBatch();
            if ((i - 1) % INSERT_BATCH_SIZE == 0) {
                statement.executeBatch();
            }
        }
        statement.executeBatch();
    }

    private List<Long> insertWithAutoGeneratedIds(List<?> objects, Connection connection, String query) throws SQLException, IllegalAccessException {
        PreparedStatement statement = connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS);
        int prevEnd = 0;
        List<Long> allIds = new ArrayList<>();
        for (int i = 0; i < objects.size(); i++) {
            if (i % INSERT_BATCH_SIZE == 0 && i > 0) {
                List<Long> ids = executeBatchAndReadIds(statement, objects, prevEnd, i);
                allIds.addAll(ids);
                prevEnd = i;
            }
            writeFields(statement, objects.get(i), false);
            statement.addBatch();
        }
        List<Long> ids = executeBatchAndReadIds(statement, objects, prevEnd, objects.size());
        allIds.addAll(ids);
        return allIds;
    }

    private List<Long> executeBatchAndReadIds(PreparedStatement statement, List<? extends Object> objects, int start, int end) throws SQLException {
        statement.executeBatch();
        ResultSet generatedKeys = statement.getGeneratedKeys();
        int ind = start;
        List<Long> ids = new ArrayList<>();
        while (generatedKeys.next()) {
            long id = generatedKeys.getLong(1);
            ids.add(id);
            try {
                databaseTypeService.writeField(objects.get(ind), id, "id");
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException("Failed to set id of object " + objects.get(ind));
            }
            ind++;
        }
        if (ind != end) {
            throw new RuntimeException("Did not retrieve enough ids after inserting objects. Retrieved " + ind + ", needed " + end);
        }
        return ids;
    }

    public void deleteObjects(Class _class, String clause, Object... args) {
        String table = getTable(_class);
        String query = "DELETE FROM " + escape(table);
        if (clause != null) {
            query += " " + clause;
        }
        String finalQuery = query;
        execute(connection -> {
            PreparedStatement statement = connection.prepareStatement(finalQuery);
            writeFields(statement, args);
            statement.execute();
            statement.close();
        });
    }

    public void writeFields(PreparedStatement statement, Object... args) throws SQLException {
        writeFields(statement, 1, args);
    }

    public void writeFields(PreparedStatement statement, int startInd, Object... args) throws SQLException {
        databaseTypeService.writeFields(startInd, statement, args, getTypesFromArgs(args));
    }

    public WrappedResultSet query(String query, Object... args) {
        return execute(connection -> {
            PreparedStatement statement = connection.prepareStatement(query);
            writeFields(statement, args);
            return new WrappedResultSet(query, connection, statement.executeQuery());
        }, true);
    }

    public void deleteObjects(Class _class) {
        deleteObjects(_class, null);
    }

    private String getFieldsString(String table, List<String> fields) {
        String cleanedTable = escape(table);
        return String.join(",", fields.stream()
                .map(this::escape)
                .map(f -> cleanedTable + "." + f)
                .collect(toList()));
    }

    public <T> List<T> readObjects(Class<T> _class) {
        return readObjects(_class, null);
    }

    public <T> List<T> readObjects(Class<T> _class, String clause, Object... args) {
        return execute(connection -> {
            return readObjects(connection, _class, clause, args);
        });
    }

    private <T> List<T> readObjects(Connection connection, Class<T> _class, String clause, Object[] args) throws SQLException {
        String query = buildQuery(_class, clause);
        PreparedStatement statement = connection.prepareStatement(query);
        writeFields(statement, args);
        statement.execute();
        List<Field> fields = getFields(_class, true).collect(toList());
        ResultSet resultSet = statement.getResultSet();
        List<T> result = new ArrayList<>();
        while (resultSet.next()) {
            noException(() -> result.add(createObject(resultSet, _class, fields)));
        }
        resultSet.close();
        statement.close();
        return result;
    }

    public <T> T readObject(Class<T> _class) {
        return singleObject(readObjects(_class));
    }

    public <T> T readObject(Class<T> _class, String clause, Object... args) {
        return singleObject(readObjects(_class, clause, args));
    }

    private <T> T singleObject(List<T> ts) {
        if (ts.isEmpty()) {
            return null;
        } else if (ts.size() == 1) {
            return ts.get(0);
        } else {
            throw new RuntimeException("Got " + ts.size() + " objects but expected zero or one object");
        }
    }

    public <T> CloseableIterator<T> readObjectsIt(Class<T> _class) {
        return readObjectsIt(_class, null);
    }

    public <T> CloseableIterator<T> readObjectsIt(Class<T> _class, String clause, Object... args) {
        String finalQuery = buildQuery(_class, clause);
        return execute(connection -> {
            PreparedStatement statement = connection.prepareStatement(finalQuery, java.sql.ResultSet.TYPE_FORWARD_ONLY, java.sql.ResultSet.CONCUR_READ_ONLY);
            statement.setFetchSize(Integer.MIN_VALUE);
            writeFields(statement, args);
            statement.execute();
            ResultSet resultSet = statement.getResultSet();
            List<Field> fields = getFields(_class, true).collect(toList());
            return new CloseableIterator<T>() {
                @Override
                protected void closeInt() {
                    noException(() -> {
                        resultSet.close();
                        statement.close();
                        connection.close();
                    });
                }

                @Override
                public boolean hasNext() {
                    return noException(resultSet::next);
                }

                @Override
                public T next() {
                    return noException(() -> createObject(resultSet, _class, fields));
                }
            };
        }, true);
    }

    private <T> T createObject(ResultSet resultSet, Class aClass, List<Field> fields) throws InvocationTargetException, NoSuchMethodException, InstantiationException, SQLException, IllegalAccessException {
        List<ReadField> fieldValues = databaseTypeService.readObjectFields(resultSet, fields);
        Class[] fieldTypes = new Class[fieldValues.size()];
        Object[] values = new Object[fieldValues.size()];
        for (int i = 0; i < fieldValues.size(); i++) {
            fieldTypes[i] = fieldValues.get(i).type;
            values[i] = fieldValues.get(i).value;
        }
        InitializationMethod<T> initializationMethod = getInitializationMethod(aClass, fields, fieldTypes);
        return initializationMethod.createObject(values);
    }

    private <T> InitializationMethod<T> getInitializationMethod(Class aClass, List<Field> fields, Class[] fieldTypes) {
        return cachedInitializationMethods.computeIfAbsent(aClass, c -> determineInitializationMethod(c, fields, fieldTypes));
    }

    private <T> InitializationMethod<T> determineInitializationMethod(Class<T> aClass, List<Field> fields, Class[] fieldTypes) {
        try {
            //Do we have single constructor for all fields?
            Constructor constructor = aClass.getConstructor(fieldTypes);
            return new AllArgsConstructor<>(constructor);
        } catch (NoSuchMethodException exp) {
            //OK
        }
        try {
            Constructor constructor = aClass.getConstructor();
            return new NoArgsConstructor<>(constructor, fields);
        } catch (NoSuchMethodException exp2) {
            throw new RuntimeException("Could not construct instance of type " + aClass + ", need a constructor without arguments, or a constructor with all arguments of same type and order as the fields");
        }
    }

    private String escape(String name) {
        return "`" + name + "`";
    }

    private String getTable(Class aClass) {
        Annotation annotation = aClass.getAnnotation(Table.class);
        if (annotation == null) {
            return aClass.getSimpleName().toLowerCase();
        } else {
            return ((Table) annotation).value();
        }
    }

    public String getFieldsString(Class _class, boolean includeId) {
        String table = getTable(_class);
        List<String> fields = getFieldNames(_class, includeId);
        return getFieldsString(table, fields);
    }

    private List<String> getFieldNames(Class _class, boolean includeId) {
        return getFields(_class, includeId).map(Field::getName).collect(toList());
    }

    public Stream<Field> getFields(Class _class, boolean includeId) {
        Field[] fields = _class.getFields();
        return Arrays.stream(fields).filter(field -> !field.getName().equals("id") || includeId);
    }

    public int writeFields(PreparedStatement statement, Object object, boolean includeId) throws IllegalAccessException, SQLException {
        List<Field> fields = getFields(object.getClass(), includeId).collect(toList());
        return writeFields(statement, object, fields);
    }

    private int writeFields(PreparedStatement statement, Object object, List<Field> fields) throws IllegalAccessException, SQLException {
        Object[] values = new Object[fields.size()];
        Class[] types = new Class[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            values[i] = field.get(object);
            types[i] = field.getType();
        }
        return databaseTypeService.writeFields(1, statement, values, types);
    }

    private String buildQuery(Class _class, String clause) {
        String table = getTable(_class);
        String query = "SELECT " + getFieldsString(_class, true) + " FROM " + escape(table);
        if (clause != null) {
            query += " " + clause;
        }
        return query;
    }

    public interface DatabaseAction {
        void execute(Connection connection) throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;
    }

    public interface DatabaseActionWithResult<T> {
        T execute(Connection connection) throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;
    }

    public interface ArgsSetter {
        void execute(PreparedStatement statement, int ind) throws SQLException;
    }

    private interface InitializationMethod<T> {
        T createObject(Object[] values) throws IllegalAccessException, InvocationTargetException, InstantiationException;
    }

    private static class AllArgsConstructor<T> implements InitializationMethod<T> {

        private final Constructor<T> constructor;

        private AllArgsConstructor(Constructor<T> constructor) {
            this.constructor = constructor;
        }

        @Override
        public T createObject(Object[] values) throws IllegalAccessException, InvocationTargetException, InstantiationException {
            return (T) constructor.newInstance(values);
        }
    }

    private static class NoArgsConstructor<T> implements InitializationMethod<T> {

        private final Constructor<T> constructor;
        private final List<Field> fields;

        private NoArgsConstructor(Constructor<T> constructor, List<Field> fields) {
            this.constructor = constructor;
            this.fields = fields;
        }

        @Override
        public T createObject(Object[] values) throws IllegalAccessException, InvocationTargetException, InstantiationException {
            T instance = (T) constructor.newInstance();
            for (int i = 0; i < fields.size(); i++) {
                fields.get(i).set(instance, values[i]);
            }
            return instance;
        }
    }

    public static class WrappedResultSet {
        private final String query;
        private final Connection connection;
        private final ResultSet resultSet;

        public WrappedResultSet(String query, Connection connection, ResultSet resultSet) {
            this.query = query;
            this.connection = connection;
            this.resultSet = resultSet;
        }

        public <T> T result(ResultSetHandler<T> handler) {
            return executeAndClose(() -> {
                if (this.resultSet.first()) {
                    return handler.handleResult(resultSet);
                } else {
                    return null;
                }
            });
        }

        private <T> T executeAndClose(ResultGenerator<T> o) {
            try {
                T result = o.generate();
                return result;
            } catch (SQLException e) {
                throw new RuntimeException("Failed to execute " + query, e);
            } finally {
                closeConnection();
            }
        }

        @Override
        protected void finalize() throws Throwable {
            closeConnection();
            super.finalize();
        }

        private void closeConnection() {
            try {
                connection.close();
            } catch (SQLException e) {
                Log.i("Received exception while closing connection", e);
            }
        }

        public <T> List<T> results(ResultSetHandler<T> handler) {
            return noException(() -> {
                List<T> results = new ArrayList<>();
                boolean moreResults = resultSet.first();
                while (moreResults) {
                    results.add(handler.handleResult(resultSet));
                    moreResults = resultSet.next();
                }
                return results;
            });
        }

        public interface ResultGenerator<T> {
            T generate() throws SQLException;
        }
    }

    public interface ResultSetHandler<T> {
        T handleResult(ResultSet resultSet) throws SQLException;
    }
}
