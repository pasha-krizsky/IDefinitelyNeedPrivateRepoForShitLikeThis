package com.pasha.oracleToCsvDataMigration.executor;

import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

@Slf4j
public final class SimpleDiscountMigrationTask
        extends AbstractMigrationTask
        implements Callable<DiscountMigrationTaskResult> {

    private static final String SELECT_ALL_QUERY_TEMPLATE = "SELECT * FROM %s";
    private static final String CSV_EXTENSION = ".csv";

    private final int taskId;
    private final String tableName;
    private final String outputDir;
    private JdbcTemplate jdbcTemplate;

    /**
     * To synchronize all submitted tasks
     */
    private final CountDownLatch countDownLatch;

    public SimpleDiscountMigrationTask(
            final String tableName,
            final int taskId,
            CountDownLatch countDownLatch,
            JdbcTemplate template,
            String outputDir) {
        this.tableName = tableName;
        this.taskId = taskId;
        this.countDownLatch = countDownLatch;
        this.jdbcTemplate = template;
        this.outputDir = outputDir;
    }

    @Override
    public DiscountMigrationTaskResult call() {

        // Waiting for submission of all tasks
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            log.error("Error while waiting for tasks submission");
            throw new RuntimeException(e);
        }

        log.info("Start TASK {}. Selecting all rows from {}", taskId, tableName);
        String query = String.format(SELECT_ALL_QUERY_TEMPLATE, tableName);
        SqlRowSet sqlRowSet = jdbcTemplate.queryForRowSet(query);
        final String csvFileName = outputDir + tableName + CSV_EXTENSION;
        final DiscountMigrationTaskResult result = writeSqlRowSetToCsv(sqlRowSet, csvFileName);
        log.info("TASK {} finished. Migrated {} rows", taskId, result.getNumberOfMigratedRows());
        return result;
    }

    private DiscountMigrationTaskResult writeSqlRowSetToCsv(SqlRowSet sqlRowSet, String csvFileName) {
        final String[] columnNames = sqlRowSet.getMetaData().getColumnNames();
        int columnCount = sqlRowSet.getMetaData().getColumnCount();
        final CsvWriter csvWriter = new CsvWriter(csvFileName);
        csvWriter.writeHeader(columnNames);
        long countMigratedRows = 0;
        while (sqlRowSet.next()) {
            List<Object> csvLine = createCsvLine(sqlRowSet, columnCount);
            csvWriter.writeRecord(csvLine);
            ++countMigratedRows;
        }
        csvWriter.flush();

        return new DiscountMigrationTaskResult(this, countMigratedRows);
    }
}
