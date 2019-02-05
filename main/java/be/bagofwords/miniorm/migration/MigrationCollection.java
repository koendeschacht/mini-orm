package be.bagofwords.miniorm.migration;

import be.bagofwords.miniorm.data.DatabaseType;

import java.util.List;

/**
 * Created by koen on 19/03/17.
 */
public interface MigrationCollection {

    List<BaseMigration> getMigrations(DatabaseType databaseType);

}
