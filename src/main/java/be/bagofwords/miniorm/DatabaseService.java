package be.bagofwords.miniorm;

import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.LifeCycleBean;
import be.bagofwords.minidepi.annotations.Inject;
import be.bagofwords.ui.UI;
import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static be.bagofwords.util.Utils.noException;
import static java.util.stream.Collectors.toList;

/**
 * Created by koen on 12.11.16.
 */
public class DatabaseService implements LifeCycleBean {

    @Inject
    private ApplicationContext context;
    @Inject
    private DatabaseTypeService databaseTypeService;

    private ComboPooledDataSource pool;

    @Override
    public void startBean() {
        pool = new ComboPooledDataSource();
        String dbHost = context.getProperty("database_host", "localhost");
        String dbName = context.getProperty("database_name");
        String dbType = context.getProperty("database_type", "mysql");
        pool.setJdbcUrl("jdbc:" + dbType + "://" + dbHost + "/" + dbName + "?verifyServerCertificate=false&useSSL=true");
        pool.setUser(context.getProperty("database_user"));
        pool.setPassword(context.getProperty("database_password"));
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
                    UI.writeError("Failed to rollback database connection!", e);
                }
            }
            throw new RuntimeException(t);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    UI.writeError("Failed to close database connection!", e);
                }
            }
        }
    }

    public <T> T execute(DatabaseActionWithResult<T> action) {
        Connection connection = null;
        try {
            connection = getConnection();
            T result = action.execute(connection);
            connection.commit();
            return result;
        } catch (Throwable t) {
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException e) {
                    UI.writeError("Failed to rollback database connection!", e);
                }
            }
            throw new RuntimeException(t);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    UI.writeError("Failed to close database connection!", e);
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

    public void insertObject(Object object) {
        insertObjects(Collections.singletonList(object));
    }

    public void insertObjects(List<? extends Object> objects) {
        if (objects.isEmpty()) {
            return;
        }
        Class objectClass = objects.get(0).getClass();
        for (Object object : objects) {
            if (object.getClass() != objectClass) {
                throw new RuntimeException("Found two types of objects " + objectClass + " and " + object.getClass());
            }
        }
        String table = getTable(objectClass);
        execute(connection -> {
            String query = "insert into " + table;
            List<String> fields = getFieldNames(objectClass, false);
            query += " (" + getFieldsString(fields) + ")";
            query += " values (" + String.join(",", fields.stream().map(name -> "?").collect(toList())) + ")";
            PreparedStatement statement = connection.prepareStatement(query);
            for (int i = 0; i < objects.size(); i++) {
                writeObjectFields(statement, objects.get(i));
                statement.addBatch();
                if (i % 100 == 0) {
                    statement.executeBatch();
                }
            }
            statement.executeBatch();
        });
    }

    public void deleteObjectsWhere(Class _class, String clause, Object... args) {
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
        deleteObjectsWhere(_class, null);
    }

    private String getFieldsString(List<String> fields) {
        return String.join(",", fields.stream().map(this::escape).collect(toList()));
    }

    public <T> List<T> readObjectsWhere(Class _class, String clause, Object... args) {
        String table = getTable(_class);
        String query = "SELECT " + getFieldsString(_class, true) + " FROM " + escape(table);
        if (clause != null) {
            query += " " + clause;
        }
        String finalQuery = query;
        return execute(connection -> {
            PreparedStatement statement = connection.prepareStatement(finalQuery);
            databaseTypeService.writeFields(1, statement, args, getTypesFromArgs(args));
            statement.execute();
            ResultSet resultSet = statement.getResultSet();
            List<T> result = new ArrayList<>();
            while (resultSet.next()) {
                List<Field> fields = getFields(_class, true).collect(toList());
                noException(() -> result.add(databaseTypeService.readObjectFields(resultSet, _class, fields)));
            }
            statement.close();
            return result;
        });
    }

    private String escape(String name) {
        return "`" + name + "`";
    }

    public <T> List<T> readObjects(Class _class) {
        return readObjectsWhere(_class, null);
    }

    private String getTable(Class aClass) {
        Annotation annotation = aClass.getAnnotation(Table.class);
        if (annotation == null) {
            throw new RuntimeException("The type " + aClass + " was not annotated with the Table annotation");
        }
        return ((Table) annotation).value();
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

    public int writeObjectFields(PreparedStatement statement, Object object) throws IllegalAccessException, SQLException {
        List<Field> fields = getFields(object.getClass(), false).collect(toList());
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

    public interface DatabaseAction {
        void execute(Connection connection) throws SQLException, InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException;
    }

    public interface DatabaseActionWithResult<T> {
        T execute(Connection connection) throws SQLException;
    }

    public interface ArgsSetter {
        void execute(PreparedStatement statement, int ind) throws SQLException;
    }

}
