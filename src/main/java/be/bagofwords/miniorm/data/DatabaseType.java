package be.bagofwords.miniorm.data;

public enum DatabaseType {
    MYSQL("mysql"), H2("h2"), ORACLE("oracle"), MSSQL("sqlserver"), OTHER(null);

    public final String jdbcType;

    DatabaseType(String jdbcType) {
        this.jdbcType = jdbcType;
    }

    public static DatabaseType fromJdbUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            throw new RuntimeException("Jdbc url is null");
        }
        if (jdbcUrl.length() == 0) {
            throw new RuntimeException("Jdbc url is empty");
        }
        String[] parts = jdbcUrl.split(":");
        if (parts.length < 2) {
            throw new RuntimeException("Can not determine database type from " + jdbcUrl);
        }
        String jdbcType = parts[1];
        for (DatabaseType databaseType : DatabaseType.values()) {
            if (databaseType.jdbcType != null && databaseType.jdbcType.equals(jdbcType)) {
                return databaseType;
            }
        }
        return OTHER;
    }
}
