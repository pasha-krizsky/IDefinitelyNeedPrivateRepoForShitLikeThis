package com.pasha.oracleToCsvDataMigration.executor;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@Slf4j
@Getter
public final class MigrationParams {

    private final String tableNamePrefix;
    private final String shards;
    private final String partitions;
    private final Integer numThreads;
    private final Integer numTableChunks;
    private final String outputDir;
    private final BigDecimal minSubsId;
    private final BigDecimal maxSubsId;
    private final Integer fetchSize;

    private final List<String> tableNamesToVisit;

    public final static class Builder {
        public static final int DEFAULT_FETCH_SIZE = 50_000;
        private String tableNamePrefix;
        private String shards;
        private String partitions;
        private Integer numThreads;
        private Integer numTableChunks;
        private String outputDir;
        private BigDecimal minSubsId;
        private BigDecimal maxSubsId;
        private Integer fetchSize;
        private List<String> tableNamesToVisit;

        public Builder tableNamePrefix(String tableNamePrefix) {
            this.tableNamePrefix = tableNamePrefix;
            return this;
        }

        public Builder shards(String shards) {
            this.shards = shards;
            return this;
        }

        public Builder partitions(String partitions) {
            this.partitions = partitions;
            return this;
        }

        public Builder numThreads(Integer numThreads) {
            this.numThreads = numThreads;
            return this;
        }

        public Builder numTableChunks(Integer numTableChunks) {
            this.numTableChunks = numTableChunks;
            return this;
        }

        public Builder outputDir(String outputDir) {
            this.outputDir = outputDir;
            return this;
        }

        public Builder minSubsId(BigDecimal minSubsId) {
            this.minSubsId = minSubsId;
            return this;
        }

        public Builder maxSubsId(BigDecimal maxSubsId) {
            this.maxSubsId = maxSubsId;
            return this;
        }

        public Builder fetchSize(Integer fetchSize) {
            this.fetchSize = fetchSize;
            return this;
        }

        public MigrationParams build() {
            processNullParams();
            buildTableNamesToVisit();
            return new MigrationParams(
                    tableNamePrefix,
                    shards,
                    partitions,
                    numThreads,
                    numTableChunks,
                    outputDir,
                    minSubsId,
                    maxSubsId,
                    fetchSize,
                    tableNamesToVisit);
        }

        private void processNullParams() {
            if (numTableChunks == null) {
                this.numTableChunks = 1;
                log.info("Param numTableChunks is not specified. Default value: {}", numTableChunks);
            }
            if (numThreads == null) {
                this.numThreads = Runtime.getRuntime().availableProcessors();
                log.info("Param numThreads is not specified. Default value: {}", numThreads);
            }
            if (fetchSize == null) {
                this.fetchSize = DEFAULT_FETCH_SIZE;
                log.info("Param fetchSize is not specified. Default value: {}", fetchSize);
            }
            if (minSubsId == null) {
                log.info("Param minSubsId is not specified. Using numTableChunks instead");
            }
            if (maxSubsId == null) {
                log.info("Param maxSubsId is not specified. Using numTableChunks instead");
            }
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

        private List<Integer> parseShards() {
            final String WRONG_SHARDS_FORMAT_MESSAGE = "Wrong shards format";
            final String TWO_DOTS_DELIMITER_REGEX = "\\.\\.";
            final String[] rawShards = shards.split(TWO_DOTS_DELIMITER_REGEX);
            if (rawShards.length != 2) {
                throw new RuntimeException(WRONG_SHARDS_FORMAT_MESSAGE);
            }
            final Integer firstShard;
            final Integer lastShard;
            try {
                firstShard = Integer.parseInt(rawShards[0]);
                lastShard = Integer.parseInt(rawShards[1]);
            } catch (NumberFormatException e) {
                throw new RuntimeException(WRONG_SHARDS_FORMAT_MESSAGE);
            }
            return Arrays.asList(firstShard, lastShard);
        }

        private List<Integer> parsePartitions() {
            final String WRONG_PARTITIONS_FORMAT_MESSAGE = "Wrong partitions format";
            final String TWO_DOTS_DELIMITER_REGEX = "\\.\\.";
            final String[] rawPartitions = partitions.split(TWO_DOTS_DELIMITER_REGEX);
            if (rawPartitions.length != 2) {
                throw new RuntimeException(WRONG_PARTITIONS_FORMAT_MESSAGE);
            }
            final Integer firstPartition;
            final Integer lastPartition;
            try {
                firstPartition = Integer.parseInt(rawPartitions[0]);
                lastPartition = Integer.parseInt(rawPartitions[1]);
            } catch (NumberFormatException e) {
                throw new RuntimeException(WRONG_PARTITIONS_FORMAT_MESSAGE);
            }
            return Arrays.asList(firstPartition, lastPartition);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    private MigrationParams(
            String tableNamePrefix,
            String shards,
            String partitions,
            Integer numThreads,
            Integer numTableChunks,
            String outputDir,
            BigDecimal minSubsId,
            BigDecimal maxSubsId,
            Integer fetchSize,
            List<String> tableNamesToVisit) {
        this.tableNamePrefix = tableNamePrefix;
        this.shards = shards;
        this.partitions = partitions;
        this.numThreads = numThreads;
        this.numTableChunks = numTableChunks;
        this.outputDir = outputDir;
        this.minSubsId = minSubsId;
        this.maxSubsId = maxSubsId;
        this.fetchSize = fetchSize;
        this.tableNamesToVisit = tableNamesToVisit;
    }
}
