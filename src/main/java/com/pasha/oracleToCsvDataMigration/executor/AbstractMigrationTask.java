package com.pasha.oracleToCsvDataMigration.executor;

import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.math.BigDecimal;
import java.sql.Types;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public abstract class AbstractMigrationTask {

    private static final String DATE_IN_CSV_PATTERN = "yyyy.MM.dd HH:mm:ss";
    private static final String DECIMAL_IN_CSV_PATTERN = "";

    protected List<Object> createCsvLine(SqlRowSet sqlRowSet, int columnCount) {
        List<Object> csvLine = new ArrayList<>();
        DateFormat dateFormatter = new SimpleDateFormat(DATE_IN_CSV_PATTERN);
//            DecimalFormatSymbols unusualSymbols = new DecimalFormatSymbols();
        DecimalFormat numFormatter = new DecimalFormat(DECIMAL_IN_CSV_PATTERN);
        for (int columnNumber = 1; columnNumber <= columnCount; ++columnNumber) {
            int type = sqlRowSet.getMetaData().getColumnType(columnNumber);
            String toWrite;
            switch (type) {
                case Types.TIMESTAMP:
                    Date valueD = sqlRowSet.getDate(columnNumber);
                    toWrite = (valueD == null ? "" : dateFormatter.format(valueD));
                    break;
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.NUMERIC:
                    BigDecimal valueN = sqlRowSet.getBigDecimal(columnNumber);
                    toWrite = (valueN == null ? "" : /*numFormatter.format(valueN)*/ valueN.toString());
                    break;
                default:
                    String valueS = sqlRowSet.getString(columnNumber);
                    toWrite = (valueS == null ? "" : valueS).replace('\n', ' ');
                    break;
            }
            csvLine.add(toWrite);
        }
        return csvLine;
    }
}
