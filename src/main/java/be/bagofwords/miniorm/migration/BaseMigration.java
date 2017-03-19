package be.bagofwords.miniorm.migration;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by koen on 19.02.17.
 */
public interface BaseMigration {

    void execute(Connection connection) throws SQLException;

    String getId();

    String getDescription();
}
