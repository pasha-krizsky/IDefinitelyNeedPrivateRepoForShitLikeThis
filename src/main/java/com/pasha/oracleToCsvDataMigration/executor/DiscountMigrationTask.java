package com.pasha.oracleToCsvDataMigration.executor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

@Slf4j
public final class DiscountMigrationTask
        extends AbstractMigrationTask
        implements Callable<DiscountMigrationTaskResult> {

    private static final String SELECT_BY_SUBS_ID_QUERY_TEMPLATE
            = "SELECT * FROM %s WHERE SUBS_SUBS_ID >= %s AND SUBS_SUBS_ID < %s";
    public static final String DISCOUNTS = "DISCOUNTS_";

    private final String tableName;
    private final BigDecimal firstSubsId;
    private final BigDecimal lastSubsId;
    private final Integer taskId;
    private final CountDownLatch countDownLatch;
    private final String outputDir;

    private JdbcTemplate jdbcTemplate;

    public DiscountMigrationTask(
            final String tableName,
            final BigDecimal firstSubsId,
            final BigDecimal lastSubsId,
            final Integer taskId,
            CountDownLatch countDownLatch,
            JdbcTemplate template,
            String outputDir) {
        this.tableName = tableName;
        this.firstSubsId = firstSubsId;
        this.lastSubsId = lastSubsId;
        this.taskId = taskId;
        this.countDownLatch = countDownLatch;
        this.jdbcTemplate = template;
        this.outputDir = outputDir;
    }

    @Override
    public DiscountMigrationTaskResult call() {
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        CsvWriter csvWriter = new CsvWriter(outputDir + DISCOUNTS + tableName + "_" + taskId);

        log.info("Task {}. Selecting from {}. Min subsId = {}. Max subsId = {}",
                lastSubsId, taskId, tableName, firstSubsId);

        String query = String.format(SELECT_BY_SUBS_ID_QUERY_TEMPLATE, tableName, firstSubsId, lastSubsId);
        SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(query);
        final String[] columnNames = sqlRowSet.getMetaData().getColumnNames();
        int columnCount = sqlRowSet.getMetaData().getColumnCount();

        csvWriter.writeHeader(columnNames);

        long countMigratedRows = 0;
        while (sqlRowSet.next()) {
            List<Object> csvLine = createCsvLine(sqlRowSet, columnCount);
            csvWriter.writeRecord(csvLine);
            ++countMigratedRows;
        }
        csvWriter.flush();
        DiscountMigrationTaskResult result = new DiscountMigrationTaskResult(this, countMigratedRows);
        log.info("Task {} finished. Migrated {} rows. Min subsId = {}. Max subsId = {}",
                taskId, columnCount, firstSubsId, lastSubsId);
        return result;
    }


}
