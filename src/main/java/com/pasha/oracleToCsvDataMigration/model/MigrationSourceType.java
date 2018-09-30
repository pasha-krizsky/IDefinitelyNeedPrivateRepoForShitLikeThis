package com.pasha.oracleToCsvDataMigration.model;

import lombok.Getter;

public enum MigrationSourceType {
    DISCOUNTS("discounts");

    @Getter
    private String value;

    MigrationSourceType(String value) {
        this.value = value;
    }
}
