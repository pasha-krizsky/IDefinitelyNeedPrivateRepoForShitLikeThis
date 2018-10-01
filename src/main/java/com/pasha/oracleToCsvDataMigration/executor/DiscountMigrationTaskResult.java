package com.pasha.oracleToCsvDataMigration.executor;

import lombok.Getter;

@Getter
public final class DiscountMigrationTaskResult {
    private final IMigrationTask discountMigrationTask;
    private final long numberOfMigratedRows;

    public DiscountMigrationTaskResult(IMigrationTask discountMigrationTask, long numberOfMigratedRows) {
        this.discountMigrationTask = discountMigrationTask;
        this.numberOfMigratedRows = numberOfMigratedRows;
    }
}
