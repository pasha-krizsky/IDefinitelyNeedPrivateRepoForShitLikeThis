package com.pasha.oracleToCsvDataMigration.command;

import com.pasha.oracleToCsvDataMigration.executor.IMigrationExecutor;
import com.pasha.oracleToCsvDataMigration.executor.MigrationParams;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;

@Slf4j
@Component
public final class MigrationCommand implements CommandMarker {

    private final IMigrationExecutor migrationExecutor;

    @Autowired
    public MigrationCommand(IMigrationExecutor migrationExecutor) {
        this.migrationExecutor = migrationExecutor;
    }

    @CliCommand(value = "migrateDiscounts")
    public void migrate(
            @CliOption(key = "tableNamePrefix", mandatory = true) final String tableName,
            @CliOption(key = "outputDir", mandatory = true) final String outputDir,
            @CliOption(key = "shards", mandatory = true) final String shards,
            @CliOption(key = "partitions", mandatory = true) final String partitions,
            @CliOption(key = "numThreads", mandatory = false) final Integer numThreads,
            @CliOption(key = "numTableChunks", mandatory = false) final Integer numTableChunks,
            @CliOption(key = "minSubsId", mandatory = false) final String minSubsId,
            @CliOption(key = "maxSubsId", mandatory = false) final String maxSubsId,
            @CliOption(key = "fetchSize", mandatory = false) final Integer fetchSize) {

        MigrationParams params = MigrationParams
                .builder()
                .tableNamePrefix(tableName)
                .shards(shards)
                .partitions(partitions)
                .numThreads(numThreads)
                .numTableChunks(numTableChunks)
                .outputDir(outputDir)
                .minSubsId(minSubsId != null ? new BigDecimal(minSubsId) : null)
                .maxSubsId(maxSubsId != null ? new BigDecimal(maxSubsId) : null)
                .fetchSize(fetchSize)
                .build();

        migrationExecutor.execute(params);
    }
}
