package be.bagofwords.miniorm.migration;

import be.bagofwords.logging.Log;
import be.bagofwords.minidepi.ApplicationContext;
import be.bagofwords.minidepi.LifeCycleBean;
import be.bagofwords.minidepi.annotations.Inject;
import be.bagofwords.miniorm.DatabaseService;

import java.sql.*;
import java.util.List;
import java.util.Objects;

import static java.util.stream.Collectors.toList;

/**
 * Created by koen on 19.02.17.
 */
public class DatabaseMigrationService implements LifeCycleBean {

    private static final String MIGRATION_TABLE = "migration";
    private static final String INITIAL_VERSION = "00000000000";

    @Inject
    private DatabaseService databaseService;
    @Inject
    private ApplicationContext applicationContext;
    @Inject
    private List<MigrationCollection> migrationCollections;

    @Override
    public void startBean() {
        try {
            runMigrations();
        } catch (SQLException e) {
            throw new RuntimeException("Failed to executed migrations.", e);
        }
    }

    @Override
    public void stopBean() {
        //Do nothing
    }

    public void runMigrations() throws SQLException {
        List<BaseMigration> migrations = collectMigrations();
        ensureMigrationTablePresent();
        String version = getVersion();
        int ind = -1;
        BaseMigration lastMigration = null;
        if (!INITIAL_VERSION.equals(version)) {
            for (int i = 0; i < migrations.size(); i++) {
                BaseMigration migration = migrations.get(i);
                if (Objects.equals(migration.getId(), version)) {
                    ind = i;
                    lastMigration = migration;
                }
            }
            if (ind == -1) {
                String errorMessage;
                if (migrations.size() == 0) {
                    errorMessage = "Last recorded migration had " + version + ", but currently no migrations are found.";
                } else {
                    errorMessage = "Last recorded migration had unknown " + version + ", see the logs for the current list of migrations";
                    for (BaseMigration migration : migrations) {
                        Log.i("\tMigration " + migration.getId() + " \"" + migration.getDescription() + "\"");
                    }
                }
                throw new IllegalStateException(errorMessage);
            }
        } else {
            Log.i("Starting from zero migrations");
        }
        int numOfMigrationsToExecute = migrations.size() - (ind + 1);
        String currentVersionMessage = "Current version of database migrations is " + version;
        if (lastMigration != null) {
            currentVersionMessage += " " + lastMigration.getDescription();
        }
        Log.i(currentVersionMessage);
        if (numOfMigrationsToExecute > 0) {
            Log.i("Executing " + numOfMigrationsToExecute + " migrations");
            for (int j = ind + 1; j < migrations.size(); j++) {
                BaseMigration migration = migrations.get(j);
                Log.i("Executing migration " + migration.getId() + " \"" + migration.getDescription() + "\"");
                this.databaseService.execute(migration::execute);
                version = migration.getId();
                updateVersion(version);
            }
        }
    }

    private void updateVersion(String version) throws SQLException {
        this.databaseService.execute(connection -> {
            PreparedStatement statement = connection.prepareStatement("update " + MIGRATION_TABLE + " set version=?");
            statement.setString(1, version);
            statement.executeUpdate();
        });
    }

    private String getVersion() throws SQLException {
        return this.databaseService.execute(connection -> {
            Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select version from " + MIGRATION_TABLE);
            if (resultSet.first()) {
                return resultSet.getString(1);
            } else {
                throw new IllegalStateException("The version table " + MIGRATION_TABLE + " does not contain a single row!");
            }
        });
    }

    private void ensureMigrationTablePresent() throws SQLException {
        this.databaseService.execute(connection -> {
            ResultSet result = connection.createStatement().executeQuery("show tables like '" + MIGRATION_TABLE + "';");
            if (!result.first()) {
                //Table does not exist yet
                Log.i("Table " + MIGRATION_TABLE + " does not exist yet, creating it...");
                connection.createStatement().execute("create table " + MIGRATION_TABLE + " ( `version` varchar(30) );");
                insertInitialVersion(connection);
            }
            //Check that the version table contains at least one row
            result.close();
            result = connection.createStatement().executeQuery("select version from " + MIGRATION_TABLE + ";");
            if (!result.first()) {
                insertInitialVersion(connection);
            }
            result.close();
        });
    }

    private void insertInitialVersion(Connection connection) throws SQLException {
        PreparedStatement statement = connection.prepareStatement("insert into " + MIGRATION_TABLE + " (version) values ('" + INITIAL_VERSION + "')");
        statement.execute();
    }

    private List<BaseMigration> collectMigrations() {
        return migrationCollections.stream().flatMap(collection -> collection.getMigrations().stream()).collect(toList());
    }

}
