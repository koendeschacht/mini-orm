package be.bagofwords.miniorm.migration;

import be.bagofwords.util.HashUtils;
import be.bagofwords.util.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Created by koen on 19.02.17.
 */
public class SimpleMigration implements BaseMigration {

    private final String statement;

    public SimpleMigration(String statement) {
        this.statement = statement;
    }

    @Override
    public void execute(Connection connection) throws SQLException {
        Statement statement = connection.createStatement();
        statement.execute(this.statement);
        statement.close();
    }

    @Override
    public String getId() {
        long hash = HashUtils.hashCode(this.statement);
        if (hash < 0) {
            hash *= -1;
        }
        return Long.toHexString(hash);
    }

    @Override
    public String getDescription() {
        return StringUtils.substring(this.statement, 0, 40);
    }
}
