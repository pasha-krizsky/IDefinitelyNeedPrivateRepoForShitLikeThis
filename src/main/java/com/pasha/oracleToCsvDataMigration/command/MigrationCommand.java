package com.pasha.oracleToCsvDataMigration.command;

import com.pasha.oracleToCsvDataMigration.executor.IMigrationExecutor;
import com.pasha.oracleToCsvDataMigration.model.MigrationParams;
import com.pasha.oracleToCsvDataMigration.model.MigrationSourceType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

@Component
public final class MigrationCommand implements CommandMarker {

    private final IMigrationExecutor migrationExecutor;

    @Autowired
    public MigrationCommand(IMigrationExecutor migrationExecutor) {
        this.migrationExecutor = migrationExecutor;
    }

    @CliCommand(value = "migrate")
    public void migrate(
            @CliOption(key = "tableName") final String tableName,
            @CliOption(key = "shards") final String shards,
            @CliOption(key = "partitions") final String partitions,
            @CliOption(key = "numThreads") final Integer numThreads,
            @CliOption(key = "numTableChunks") final Integer numTableChunks,
            @CliOption(key = "outputDir") final String outputDir,
            @CliOption(key = "type") final String type) {
        MigrationParams params = MigrationParams
                .builder()
                .tableName(tableName)
                .shards(shards)
                .partitions(partitions)
                .numThreads(numThreads)
                .numTableChunks(numTableChunks)
                .outputDir(outputDir)
                .type(type)
                .build();

        MigrationSourceType sourceType = MigrationSourceType.valueOf(params.getType().toUpperCase());
        switch (sourceType) {
            case DISCOUNTS:
                migrationExecutor.execute(params);
                break;
            default:
                System.out.println("Unknown source type!");
        }
    }
}
