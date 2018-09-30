package com.pasha.oracleToCsvDataMigration.model;

import lombok.Getter;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Getter
public final class MigrationParams {

    private final String tableNamePrefix;
    private final String shards;
    private final String partitions;
    private final Integer numThreads;
    private final Integer numTableChunks;
    private final String outputDir;
    private final String type;

    private final List<String> tableNamesToVisit;

    public final static class Builder {
        private String tableNamePrefix;
        private String shards;
        private String partitions;
        private Integer numThreads;
        private Integer numTableChunks;
        private String outputDir;
        private String type;
        private List<String> tableNamesToVisit;

        public @NonNull Builder tableName(@NonNull String tableNamePrefix) {
            this.tableNamePrefix = tableNamePrefix;
            return this;
        }

        public @NonNull Builder shards(@Nullable String shards) {
            this.shards = shards;
            return this;
        }

        public @NonNull Builder partitions(@Nullable String partitions) {
            this.partitions = partitions;
            return this;
        }

        public @NonNull Builder numThreads(@Nullable Integer numThreads) {
            this.numThreads = numThreads;
            return this;
        }

        public @NonNull Builder numTableChunks(@Nullable Integer numTableChunks) {
            this.numTableChunks = numTableChunks;
            return this;
        }

        public @NonNull Builder outputDir(@NonNull String outputDir) {
            this.outputDir = outputDir;
            return this;
        }

        public @NonNull Builder type(@NonNull String type) {
            this.type = type;
            return this;
        }

        public @NonNull MigrationParams build() {
            if (numTableChunks == null) {
                this.numTableChunks = 1;
            }
            if (numThreads == null) {
                this.numThreads = Runtime.getRuntime().availableProcessors();
            }
            buildTableNamesToVisit();
            return new MigrationParams(
                    tableNamePrefix,
                    shards,
                    partitions,
                    numThreads,
                    numTableChunks,
                    outputDir,
                    type,
                    tableNamesToVisit);
        }

        private void buildTableNamesToVisit() {
            final String TABLE_PREFIX_AND_SHARD_DELIMITER = "_";
            final String SHARD_AND_PARTITION_DELIMITER = "_";
            final String FORMAT_FOR_SHARD = "%03d";
            final String FORMAT_FOR_PARTITION = "%03d";

            final List<Integer> firstAndLastShards = parseShards();
            final List<Integer> firstAndLastPartitions = parsePartitions();
            tableNamesToVisit = new ArrayList<>();
            final Integer firstShard = firstAndLastShards.get(0);
            final Integer lastShard = firstAndLastShards.get(1);
            for (int currentShard = firstShard; currentShard <= lastShard; ++currentShard) {
                final Integer firstPartition = firstAndLastPartitions.get(0);
                final Integer lastPartition = firstAndLastPartitions.get(1);
                for (int currentPartition = firstPartition; currentPartition <= lastPartition; ++currentPartition) {
                    tableNamesToVisit.add(tableNamePrefix +
                            TABLE_PREFIX_AND_SHARD_DELIMITER +
                            String.format(FORMAT_FOR_SHARD, currentShard) +
                            SHARD_AND_PARTITION_DELIMITER +
                            String.format(FORMAT_FOR_PARTITION, currentPartition));
                }
            }
        }

        private @NonNull List<Integer> parseShards() {
            final String WRONG_SHARDS_FORMAT_MESSAGE = "Wrong shards format";
            return parseShardsOrPartitions(WRONG_SHARDS_FORMAT_MESSAGE);
        }

        private @NonNull List<Integer> parsePartitions() {
            final String WRONG_PARTITIONS_FORMAT_MESSAGE = "Wrong partitions format";
            return parseShardsOrPartitions(WRONG_PARTITIONS_FORMAT_MESSAGE);
        }

        private @NonNull List<Integer> parseShardsOrPartitions(@NonNull String errorMessage) {
            final String TWO_DOTS_DELIMITER_REGEX = "\\.\\.";
            final String[] rawPartitions = partitions.split(TWO_DOTS_DELIMITER_REGEX);
            if (rawPartitions.length != 2) {
                throw new RuntimeException(errorMessage);
            }
            final Integer firstPartition;
            final Integer lastPartition;
            try {
                firstPartition = Integer.parseInt(rawPartitions[0]);
                lastPartition = Integer.parseInt(rawPartitions[1]);
            } catch (NumberFormatException e) {
                throw new RuntimeException(errorMessage);
            }
            return Arrays.asList(firstPartition, lastPartition);
        }
    }

    public static @NonNull Builder builder() {
        return new Builder();
    }

    private MigrationParams(
            @NonNull String tableName,
            @Nullable String shards,
            @Nullable String partitions,
            @Nullable Integer numThreads,
            @NonNull Integer numTableChunks,
            @NonNull String outputDir,
            @NonNull String type,
            @NonNull List<String> tableNamesToVisit) {
        this.tableNamePrefix = tableName;
        this.shards = shards;
        this.partitions = partitions;
        this.numThreads = numThreads;
        this.numTableChunks = numTableChunks;
        this.outputDir = outputDir;
        this.type = type;
        this.tableNamesToVisit = tableNamesToVisit;
    }
}
