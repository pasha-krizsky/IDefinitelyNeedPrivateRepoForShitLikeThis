package com.pasha.oracleToCsvDataMigration.executor;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

public class DiscountMigrationTask implements Callable<DiscountMigrationTaskResult> {

    private static final String SELECT_BY_SUBS_ID_QUERY_TEMPLATE = "SELECT * FROM %s WHERE SUBS_SUBS_ID >= %d AND SUBS_SUBS_ID < %d";

    private final String tableName;
    private final Integer firstSubsId;
    private final Integer lastSubsId;
    private final Integer taskId;
    private final CountDownLatch countDownLatch;

    private DiscountMigrationTaskResult result;

    private JdbcTemplate jdbcTemplate;

    public DiscountMigrationTask(
            final String tableName,
            final Integer firstSubsId,
            final Integer lastSubsId,
            final Integer taskId,
            CountDownLatch countDownLatch,
            JdbcTemplate template) {
        this.tableName = tableName;
        this.firstSubsId = firstSubsId;
        this.lastSubsId = lastSubsId;
        this.taskId = taskId;
        this.countDownLatch = countDownLatch;
        this.jdbcTemplate = template;
    }

    @Override
    public DiscountMigrationTaskResult call() {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        System.out.println("Task " + taskId + ". Selecting from " + tableName
                + ". Min subsId = " + firstSubsId + ". Max subsId = " + lastSubsId);
        String query = String.format(SELECT_BY_SUBS_ID_QUERY_TEMPLATE, tableName, firstSubsId, lastSubsId);
        SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(query);
        // TODO Add processing (write to CSV)
        int columnCount = sqlRowSet.getMetaData().getColumnCount();
        result = new DiscountMigrationTaskResult(this, columnCount);
        System.out.println("Task " + taskId + " finished");
        return result;
    }
}
