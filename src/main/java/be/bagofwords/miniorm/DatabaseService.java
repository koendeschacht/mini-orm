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
        String jdbcUrl = "jdbc:" + dbType + "://" + dbHost + "/" + dbName + "?verifyServerCertificate=false&useSSL=true";
        if (StringUtils.isNotEmpty(extraArgs)) {
            jdbcUrl += "&" + extraArgs;
        }
        Log.i("Initiating database connection " + jdbcUrl);
        pool.setJdbcUrl(jdbcUrl);
        pool.setUser(context.getProperty("database.user"));
        pool.setPassword(context.getProperty("database.password"));
        pool.setMaxPoolSize(20);
        pool.setMinPoolSize(5);
        pool.setInitialPoolSize(5);
        pool.setAutoCommitOnClose(true);
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
        String table = getTable(object.getClass());
        execute(connection -> {
            String query = "update " + table + " set ";
            List<Field> fields = getFields(object.getClass(), true).collect(toList());
            Field idField = null;
            List<Field> fieldsInOrder = new ArrayList<>();
            for (Field field : fields) {
                if (field.getName().equals("id")) {
                    idField = field;
                } else {
                    query += escape(field.getName()) + "=? ";
                    fieldsInOrder.add(field);
                }
            }
            if (idField == null) {
                throw new RuntimeException("Could not find id field for object " + object);
            }
            fieldsInOrder.add(idField);
            query += "where id=?";
            PreparedStatement statement = connection.prepareStatement(query);
            writeObjectFields(statement, object, fieldsInOrder);
            statement.executeBatch();
        });
    }

    public Long insertObject(Object object, boolean generateId) {
        List<Long> ids = insertObjects(Collections.singletonList(object), generateId);
        if (generateId) {
            return ids.get(0);
        } else {
            return null;
        }
    }

    public Long insertObject(Object object) {
        return insertObject(object, true);
    }

    public List<Long> insertObjects(List<? extends Object> objects) {
        return insertObjects(objects, true);
    }

    public List<Long> insertObjects(List<? extends Object> objects, boolean generateId) {
        if (objects.isEmpty()) {
            if (generateId) {
                return Collections.emptyList();
            } else {
                return null;
            }
        }
        Class objectClass = objects.get(0).getClass();
        for (Object object : objects) {
            if (object.getClass() != objectClass) {
                throw new RuntimeException("Found two types of objects " + objectClass + " and " + object.getClass());
            }
        }
        checkObjectClassHasCorrectId(objectClass);
        String table = getTable(objectClass);
        return execute(connection -> {
            String query = "insert into " + table;
            List<String> fields = getFieldNames(objectClass, !generateId);
            query += " (" + getFieldsString(fields) + ")";
            query += " values (" + String.join(",", fields.stream().map(name -> "?").collect(toList())) + ")";
            boolean hasId;
            try {
                objectClass.getField("id");
                hasId = true;
            } catch (NoSuchFieldException exp) {
                hasId = false;
            }
            if (hasId && generateId) {
                return insertWithAutoGeneratedIds(objects, connection, query);
            } else {
                insertWithoutAutoGeneratedIds(objects, connection, query);
                return null;
            }
        });
    }

    private void checkObjectClassHasCorrectId(Class objectClass) {
        try {
            Field idField = objectClass.getField("id");
            if (!idField.getType().equals(Long.class) && !idField.getType().equals(long.class)) {
                throw new RuntimeException("The id field of class " + objectClass + " is not of type Long or long");
            }
        } catch (NoSuchFieldException exp) {
            //Ok
        }
    }

    private void insertWithoutAutoGeneratedIds(List<?> objects, Connection connection, String query) throws SQLException, IllegalAccessException {
        PreparedStatement statement = connection.prepareStatement(query);
        for (int i = 0; i < objects.size(); i++) {
            writeObjectFields(statement, objects.get(i), true);
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
            writeObjectFields(statement, objects.get(i), false);
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
            databaseTypeService.writeFields(1, statement, args, getTypesFromArgs(args));
            statement.execute();
            statement.close();
        });
    }

    public void deleteObjects(Class _class) {
        deleteObjects(_class, null);
    }

    private String getFieldsString(List<String> fields) {
        return String.join(",", fields.stream().map(this::escape).collect(toList()));
    }

    public <T> List<T> readObjects(Class<T> _class) {
        return readObjects(_class, null);
    }

    public <T> List<T> readObjects(Class<T> _class, String clause, Object... args) {
        String query = buildQuery(_class, clause);
        return execute(connection -> {
            PreparedStatement statement = connection.prepareStatement(query);
            databaseTypeService.writeFields(1, statement, args, getTypesFromArgs(args));
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
        });
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
            databaseTypeService.writeFields(1, statement, args, getTypesFromArgs(args));
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
        List<String> fields = getFieldNames(_class, includeId);
        return getFieldsString(fields);
    }

    private List<String> getFieldNames(Class _class, boolean includeId) {
        return getFields(_class, includeId).map(Field::getName).collect(toList());
    }

    public Stream<Field> getFields(Class _class, boolean includeId) {
        Field[] fields = _class.getFields();
        return Arrays.stream(fields).filter(field -> !field.getName().equals("id") || includeId);
    }

    public int writeObjectFields(PreparedStatement statement, Object object, boolean includeId) throws IllegalAccessException, SQLException {
        List<Field> fields = getFields(object.getClass(), includeId).collect(toList());
        return writeObjectFields(statement, object, fields);
    }

    private int writeObjectFields(PreparedStatement statement, Object object, List<Field> fields) throws IllegalAccessException, SQLException {
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

}
