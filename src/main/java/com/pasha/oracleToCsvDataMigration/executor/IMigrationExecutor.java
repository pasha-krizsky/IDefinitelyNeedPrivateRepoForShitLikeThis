package com.pasha.oracleToCsvDataMigration.executor;

import com.pasha.oracleToCsvDataMigration.model.MigrationParams;
import org.springframework.lang.NonNull;

public interface IMigrationExecutor {
    void execute(@NonNull MigrationParams params);
}
