package com.pasha.oracleToCsvDataMigration.executor;

import com.pasha.oracleToCsvDataMigration.model.MigrationParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
public final class MigrationDiscountsExecutor implements IMigrationExecutor {

    private static final String MIN_MAX_SUBS_ID_QUERY_TEMPLATE = "SELECT MIN(SUBS_SUBS_ID), MAX(SUBS_SUBS_ID) from %s";

    private static final String MIN_SUBS_ID_COLUMN_NAME = "MIN(SUBS_SUBS_ID)";
    private static final String MAX_SUBS_ID_COLUMN_NAME = "MAX(SUBS_SUBS_ID)";

    private final JdbcTemplate jdbcTemplate;
    private CountDownLatch countDownLatch;

    @Autowired
    public MigrationDiscountsExecutor(@NonNull JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        countDownLatch = new CountDownLatch(1);
    }

    @Override
    public void execute(@NonNull MigrationParams params) {
        int numberSubmittedTasks = 0;

        List<String> tableNames = params.getTableNamesToVisit();

        Integer numThreads = params.getNumThreads();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        ExecutorCompletionService<DiscountMigrationTaskResult> completionService = new ExecutorCompletionService<>(executor);

        for (String tableName : tableNames) {
            List<Integer> minAndMaxSubsIds = findMinAndMaxSubsIds(params, tableName);
            Integer minSubsId = minAndMaxSubsIds.get(0);
            Integer maxSubsId = minAndMaxSubsIds.get(1);
            Integer numTableChunks = params.getNumTableChunks();

            Integer stepSubsId = (maxSubsId - minSubsId) / numTableChunks;
            Integer currentSubsId = minSubsId;

            while (currentSubsId <= maxSubsId) {
                Integer nextSubsId = currentSubsId + stepSubsId;
                DiscountMigrationTask task = new DiscountMigrationTask(
                        tableName,
                        currentSubsId,
                        nextSubsId,
                        numberSubmittedTasks,
                        countDownLatch,
                        jdbcTemplate);
                completionService.submit(task);
                ++numberSubmittedTasks;
                currentSubsId = nextSubsId;
            }
        }

        System.out.println("-----------------------------------------------");
        System.out.println("Finish submitting tasks...");
        System.out.println("Number of tables to be migrated: " + tableNames.size());
        System.out.println("Number of submitted tasks: " + numberSubmittedTasks);
        System.out.println("-----------------------------------------------");
        System.out.println("Start performing tasks with " + numThreads + " threads");

        countDownLatch.countDown();
        for (int i = 0; i < numberSubmittedTasks; ++i) {
            try {
                DiscountMigrationTaskResult result = completionService.take().get();
                // TODO Processing result
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private @NonNull
    List<Integer> findMinAndMaxSubsIds(@NonNull MigrationParams params, @NonNull String tableName) {
        List<Integer> res = new ArrayList<>(2);
        List<String> tableNamesToVisit = params.getTableNamesToVisit();

        if (CollectionUtils.isEmpty(tableNamesToVisit)) {
            throw new NullPointerException("Table name was missed or generated incorrectly");
        }

        System.out.println("Searching for min and max SUBS_SUBS_ID values...");
        String query = String.format(MIN_MAX_SUBS_ID_QUERY_TEMPLATE, tableName);
        SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(query);
        sqlRowSet.next();

        Integer minSubsId = sqlRowSet.getInt(MIN_SUBS_ID_COLUMN_NAME);
        res.add(minSubsId);
        System.out.println("Min SUBS_SUBS_ID value: " + minSubsId);
        Integer maxSubsId = sqlRowSet.getInt(MAX_SUBS_ID_COLUMN_NAME);
        res.add(maxSubsId);
        System.out.println("Max SUBS_SUBS_ID value: " + maxSubsId);

        return res;
    }
}
