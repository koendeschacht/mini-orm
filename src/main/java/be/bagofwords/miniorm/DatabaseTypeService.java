package be.bagofwords.miniorm;

import be.bagofwords.miniorm.data.ReadField;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by koen on 19/03/17.
 */
public class DatabaseTypeService {

    public void writeField(Object object, Object value, String name) throws SQLException, NoSuchFieldException, IllegalAccessException {
        Class<?> objClass = object.getClass();
        Field field = objClass.getField(name);
        field.setAccessible(true);
        field.set(object, value);
    }

    public int writeFields(int startInd, PreparedStatement statement, Object[] values, Class[] types) throws SQLException {
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
        return startInd + values.length;
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

    public List<ReadField> readObjectFields(ResultSet resultSet, List<Field> fields) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException, SQLException {
        List<ReadField> values = new ArrayList<>();
        for (int i = 0; i < fields.size(); i++) {
            Field field = fields.get(i);
            int ind = i + 1;
            Class<?> type = field.getType();
            Object value;
            if (type.equals(Integer.class) || type.equals(int.class)) {
                value = resultSet.getInt(ind);
            } else if (type.equals(Long.class) || type.equals(long.class)) {
                value = resultSet.getLong(ind);
            } else if (type.equals(Boolean.class) || type.equals(boolean.class)) {
                value = resultSet.getBoolean(ind);
            } else if (type.equals(Double.class) || type.equals(double.class)) {
                value = resultSet.getDouble(ind);
            } else if (type.equals(String.class)) {
                value = resultSet.getString(ind);
            } else if (type.equals(Date.class)) {
                Timestamp timestamp = resultSet.getTimestamp(ind);
                if (timestamp == null) {
                    value = null;
                } else {
                    value = new Date(timestamp.getTime());
                }
            } else {
                throw new RuntimeException("Unknown type " + type);
            }
            values.add(new ReadField(value, type));
        }
        return values;
    }

}
