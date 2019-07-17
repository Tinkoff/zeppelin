package ru.tinkoff.zeppelin.interpreter.content;


import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.LinkedHashMap;

public class H2TableConverter {
    private int rowsCount = 0;

    public H2Table resultSetToTable(ResultSet resultSet,
                                    final String tableName,
                                    final H2TableType h2TableType,
                                    final int rowCount,
                                    final int rowLimit) {
        try {
            final H2TableHeader header = new H2TableHeader(tableName, h2TableType);
            final ArrayList<ArrayList<Object>> table = getTable(resultSet, rowCount, rowLimit);
            final H2TableMetadata metadata = new H2TableMetadata(getColumns(resultSet.getMetaData()), rowsCount);
            return new H2Table(header, metadata, table);
        } catch (final Exception ex) {
            return null;
        }
    }


    private ArrayList<ArrayList<Object>> getTable(final ResultSet resultSet,
                                                  final int rowCount,
                                                  final int rowLimit) {
        rowsCount = 0;
        try {
            final int columnNumber = resultSet.getMetaData().getColumnCount();
            final ArrayList<ArrayList<Object>> table = new ArrayList<>();
            for (int i = 0; i < columnNumber; i++) {
                table.add(new ArrayList<>());
            }
            while ((rowLimit == 0 || resultSet.getRow() < rowLimit) && resultSet.next()) {
                for (int i = 0; i < columnNumber; i++) {
                    switch (resultSet.getMetaData().getColumnType(i + 1)) {
                        case 4:
                        case 5:
                        case -5:
                            table.get(i).add(resultSet.getInt(i + 1));
                            break;
                        case 6:
                        case 7:
                        case 8:
                            table.get(i).add(resultSet.getDouble(i + 1));
                            break;
                        case 16:
                            table.get(i).add(resultSet.getBoolean(i + 1));
                            break;
                        case 12:
                        case 1:
                            table.get(i).add(resultSet.getString(i + 1));
                            break;
                        default:
                            table.get(i).add(resultSet.getString(i + 1));
                            break;
                    }
                }
                rowsCount++;
            }
            rowsCount = rowCount == -1L ? rowsCount : rowCount;
            return table;
        } catch (
                final Exception ex) {
            return null;
        }

    }

    public LinkedHashMap<String, String> getColumns(final ResultSetMetaData md) throws SQLException {
        final LinkedHashMap<String, String> columns = new LinkedHashMap<>();
        for (int i = 1; i < md.getColumnCount() + 1; i++) {
            columns.put(
                    StringUtils.isNotEmpty(md.getColumnLabel(i))
                            ? md.getColumnLabel(i)
                            : md.getColumnName(i),
                    md.getColumnTypeName(i)
            );
        }
        return columns;
    }
}
