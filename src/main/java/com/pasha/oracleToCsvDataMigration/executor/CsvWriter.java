package com.pasha.oracleToCsvDataMigration.executor;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class CsvWriter {
    private CSVPrinter csvPrinter;
    private BufferedWriter writer;

    public CsvWriter(String path) {
        try {
            writer = Files.newBufferedWriter(Paths.get(path));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeHeader(String[] header) {
        try {
            csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT
                    .withHeader(header));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void writeRecord(List<Object> columns) {
        try {
            csvPrinter.printRecord(columns);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        try {
            csvPrinter.flush();
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
