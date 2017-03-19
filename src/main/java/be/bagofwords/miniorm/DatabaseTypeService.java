package be.bagofwords.miniorm;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.Date;
import java.util.List;

/**
 * Created by koen on 19/03/17.
 */
public class DatabaseTypeService {

    public void writeFields(int startInd, PreparedStatement statement, Object[] values, Class[] types) throws SQLException {
        for (int i = 0; i < values.length; i++) {
            Object value = values[i];
            Class<?> type = types[i];
            int ind = i + startInd;
            if (value == null) {
                statement.setNull(ind, getSQLType(type));
                continue;
            }
            if (type.equals(Integer.class) || type.equals(int.class)) {
                statement.setInt(ind, (Integer) value);
            } else if (type.equals(Long.class) || type.equals(long.class)) {
                statement.setLong(ind, (Long) value);
            } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
                statement.setBoolean(ind, (Boolean) value);
            } else if (type.equals(Double.class) || type.equals(double.class)) {
                statement.setDouble(ind, (Double) value);
            } else if (type.equals(String.class)) {
                statement.setString(ind, (String) value);
            } else if (type.equals(Date.class)) {
                statement.setTimestamp(ind, new Timestamp(((Date) value).getTime()));
            } else {
                throw new RuntimeException("Unknown type " + type);
            }
        }
    }

    public int getSQLType(Class<?> type) {
        if (type.equals(Integer.class) || type.equals(int.class)) {
            return Types.INTEGER;
        } else if (type.equals(Long.class) || type.equals(long.class)) {
            return Types.BIGINT;
        } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
            return Types.BOOLEAN;
        } else if (type.equals(Double.class) || type.equals(double.class)) {
            return Types.DOUBLE;
        } else if (type.equals(String.class)) {
            return Types.VARCHAR;
        } else if (type.equals(Date.class)) {
            return Types.DATE;
        } else {
            throw new RuntimeException("Unknown type " + type);
        }
    }

    public <T> T readObjectFields(ResultSet resultSet, Class _class, List<Field> fields) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, SQLException {
        T object = (T) _class.getConstructor().newInstance();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            int ind = i + 1;
            Class<?> type = field.getType();
            if (type.equals(Integer.class) || type.equals(int.class)) {
                field.set(object, resultSet.getInt(ind));
            } else if (type.equals(Long.class) || type.equals(long.class)) {
                field.set(object, resultSet.getLong(ind));
            } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
                field.set(object, resultSet.getBoolean(ind));
            } else if (type.equals(Double.class) || type.equals(double.class)) {
                field.set(object, resultSet.getDouble(ind));
            } else if (type.equals(String.class)) {
                field.set(object, resultSet.getString(ind));
            } else if (type.equals(Date.class)) {
                field.set(object, new Date(resultSet.getTimestamp(ind).getTime()));
            } else {
                throw new RuntimeException("Unknown type " + type);
            }
        }
        return object;
    }

}
