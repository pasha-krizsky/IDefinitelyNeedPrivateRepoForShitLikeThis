package com.pasha.oracleToCsvDataMigration.executor;

import lombok.Getter;

@Getter
public final class DiscountMigrationTaskResult {
    private final AbstractMigrationTask discountMigrationTask;
    private final long numberOfMigratedRows;

    public DiscountMigrationTaskResult(AbstractMigrationTask discountMigrationTask, long numberOfMigratedRows) {
        this.discountMigrationTask = discountMigrationTask;
        this.numberOfMigratedRows = numberOfMigratedRows;
    }
}
