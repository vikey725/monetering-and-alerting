package com.chargemon.schema;

import com.chargemon.common.config.Env;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.output.MigrateResult;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runs Flyway migrations. Intended as a one-shot container / k8s Job before
 * the Flink job and the notifier start.
 */
public final class SchemaMigrator {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaMigrator.class);

    private SchemaMigrator() {
    }

    public static void main(String[] args) {
        Env env = Env.system();
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setUrl(env.get("DB_URL", "jdbc:postgresql://localhost:5432/chargemon"));
        ds.setUser(env.get("DB_USER", "chargemon"));
        ds.setPassword(env.get("DB_PASSWORD", "chargemon"));
        MigrateResult result = migrate(ds);
        LOG.info("Applied {} migration(s); schema at {}", result.migrationsExecuted, result.targetSchemaVersion);
    }

    public static MigrateResult migrate(DataSource dataSource) {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .baselineOnMigrate(true)
                .validateMigrationNaming(true)
                .load();
        return flyway.migrate();
    }
}
