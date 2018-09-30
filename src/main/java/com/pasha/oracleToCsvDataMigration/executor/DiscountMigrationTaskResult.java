package com.pasha.oracleToCsvDataMigration.executor;

import lombok.Getter;

@Getter
public final class DiscountMigrationTaskResult {
    private final DiscountMigrationTask discountMigrationTask;
    private final Integer numberOfMigratedRows;

    public DiscountMigrationTaskResult(DiscountMigrationTask discountMigrationTask, Integer numberOfMigratedRows) {
        this.discountMigrationTask = discountMigrationTask;
        this.numberOfMigratedRows = numberOfMigratedRows;
    }
}
