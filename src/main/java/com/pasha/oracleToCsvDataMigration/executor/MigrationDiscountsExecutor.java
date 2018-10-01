package com.pasha.oracleToCsvDataMigration.executor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@Slf4j
@Component
public final class MigrationDiscountsExecutor implements IMigrationExecutor {

    private static final String MIN_MAX_SUBS_ID_QUERY_TEMPLATE = "SELECT MIN(SUBS_SUBS_ID), MAX(SUBS_SUBS_ID) from %s";
    private static final String MIN_SUBS_ID_COLUMN_NAME = "MIN(SUBS_SUBS_ID)";
    private static final String MAX_SUBS_ID_COLUMN_NAME = "MAX(SUBS_SUBS_ID)";
    private static final String LOCAL_DATE_PATTERN = "yyyy/MM/dd HH:mm:ss";

    private CountDownLatch countDownLatch;
    private final JdbcTemplate jdbcTemplate;

    @Autowired
    public MigrationDiscountsExecutor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        countDownLatch = new CountDownLatch(1);
    }

    @Override
    public void execute(MigrationParams params) {
        jdbcTemplate.setFetchSize(params.getFetchSize());
        List<String> tableNames = params.getTableNamesToVisit();
        Integer numThreads = params.getNumThreads();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<DiscountMigrationTaskResult> completionService
                = new ExecutorCompletionService<>(executor);
        Integer numTableChunks = params.getNumTableChunks();

        int numberSubmittedTasks;
        numberSubmittedTasks = numTableChunks.equals(1) ?
                submitSimpleDiscountMigrationTasks(params, completionService) :
                submitDiscountMigrationTasks(params, completionService);

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern(LOCAL_DATE_PATTERN);
        LocalDateTime startTime = LocalDateTime.now();
        log.info("-----------------------------------------------");
        log.info("Finish submitting tasks...");
        log.info("Number of tables to be migrated: {}", tableNames.size());
        log.info("Number of submitted tasks: {}", numberSubmittedTasks);
        log.info("-----------------------------------------------");
        log.info("Start performing tasks with {} threads", numThreads);

        countDownLatch.countDown();

        BigDecimal numberOfMigratedRows = new BigDecimal(0);
        for (int i = 0; i < numberSubmittedTasks; ++i) {
            try {
                DiscountMigrationTaskResult result = completionService.take().get();
                numberOfMigratedRows = numberOfMigratedRows.add(new BigDecimal(result.getNumberOfMigratedRows()));
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        LocalDateTime endTime = LocalDateTime.now();

        log.info("Migration finished!");
        log.info("Total number of migrated rows: {}", numberOfMigratedRows);
        log.info("Start time: {}", dtf.format(startTime));
        log.info("End time: {}", dtf.format(endTime));
    }

    private int submitSimpleDiscountMigrationTasks(
            MigrationParams params,
            ExecutorCompletionService<DiscountMigrationTaskResult> completionService) {
        int numberSubmittedTasks = 0;
        for (String table : params.getTableNamesToVisit()) {
            SimpleDiscountMigrationTask task = new SimpleDiscountMigrationTask(
                    table,
                    numberSubmittedTasks,
                    countDownLatch,
                    jdbcTemplate,
                    params.getOutputDir());
            completionService.submit(task);
            ++numberSubmittedTasks;
        }
        return numberSubmittedTasks;
    }

    private int submitDiscountMigrationTasks(
            MigrationParams params,
            ExecutorCompletionService<DiscountMigrationTaskResult> completionService) {
        int numberSubmittedTasks = 0;
        BigDecimal minSubsId = params.getMinSubsId();
        BigDecimal maxSubsId = params.getMaxSubsId();
        if (minSubsId == null || maxSubsId == null) {
            for (String tableName : params.getTableNamesToVisit()) {
                List<BigDecimal> minAndMaxSubsIds = findMinAndMaxSubsIds(params, tableName);
                minSubsId = minAndMaxSubsIds.get(0);
                maxSubsId = minAndMaxSubsIds.get(1);

                if (minSubsId == null || maxSubsId == null) {
                    continue;
                }

                BigDecimal stepSubsId = calculateStep(minSubsId, maxSubsId, params.getNumTableChunks());
                BigDecimal currentSubsId = minSubsId;

                while (currentSubsId.compareTo(maxSubsId) <= 0) {
                    BigDecimal nextSubsId = currentSubsId.add(stepSubsId);
                    DiscountMigrationTask task = new DiscountMigrationTask(
                            tableName,
                            currentSubsId,
                            nextSubsId,
                            numberSubmittedTasks,
                            countDownLatch,
                            jdbcTemplate,
                            params.getOutputDir());
                    completionService.submit(task);
                    ++numberSubmittedTasks;
                    currentSubsId = nextSubsId;
                }
            }
        }
        return numberSubmittedTasks;
    }

    private List<BigDecimal> findMinAndMaxSubsIds(MigrationParams params, String tableName) {
        List<BigDecimal> res = new ArrayList<>(2);
        List<String> tableNamesToVisit = params.getTableNamesToVisit();

        if (CollectionUtils.isEmpty(tableNamesToVisit)) {
            throw new NullPointerException("Table name was missed or generated incorrectly");
        }

        log.info("Searching for min and max SUBS_SUBS_ID values");
        String query = String.format(MIN_MAX_SUBS_ID_QUERY_TEMPLATE, tableName);
        SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(query);
        sqlRowSet.next();
        BigDecimal minSubsId = sqlRowSet.getBigDecimal(MIN_SUBS_ID_COLUMN_NAME);
        res.add(minSubsId);
        log.info("Min SUBS_SUBS_ID value: {}", minSubsId);
        BigDecimal maxSubsId = sqlRowSet.getBigDecimal(MAX_SUBS_ID_COLUMN_NAME);
        res.add(maxSubsId);
        log.info("Max SUBS_SUBS_ID value: {}", maxSubsId);

        return res;
    }

    private BigDecimal calculateStep(BigDecimal min, BigDecimal max, Integer numOfChunks) {
        return max.subtract(min).divide(BigDecimal.valueOf(numOfChunks), BigDecimal.ROUND_UP);
    }
}
