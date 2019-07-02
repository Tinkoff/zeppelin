package ru.tinkoff.zeppelin.commons.jdbc;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class H2Manager {

    public enum Type {
        REAL("R_TABLES"),
        VIRTUAL("V_TABLES"),
        ;

        private final String schemaName;

        Type(final String schemaName) {
            this.schemaName = schemaName;
        }

        public String getSchemaName() {
            return schemaName;
        }
    }

    public String saveTable(final String schema,
                            final String tableName,
                            final ResultSet resultSet,
                            long rowCount,
                            final long rowLimit,
                            final Connection h2Connection) throws Exception {
        try {
            if (tableName.isEmpty()) {
                return null;
            }
            final String resultString;


            // create schema if not exists
            h2Connection.createStatement().execute(String.format("CREATE SCHEMA IF NOT EXISTS %s;", schema));

            // drop tables
            h2Connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s.%s;", schema, tableName));
            h2Connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s.%s_META;", schema, tableName));

            // create tables
            final StringBuilder createTable = new StringBuilder();
            createTable.append("CREATE TABLE ").append(schema).append(".").append(tableName).append(" ( \n");

            final List<String> columns = new ArrayList<>();
            final ResultSetMetaData md = resultSet.getMetaData();
            for (int i = 1; i < md.getColumnCount() + 1; i++) {
                columns.add(md.getColumnName(i));

                createTable.append(
                        StringUtils.isNotEmpty(md.getColumnLabel(i))
                                ? md.getColumnLabel(i)
                                : md.getColumnName(i)
                );
                createTable.append(" \t");

                createTable.append(md.getColumnTypeName(i));

                if (i != md.getColumnCount()) {
                    createTable.append(", \n");
                }
            }
            createTable.append(");");

            h2Connection.createStatement().execute(createTable.toString());

            // create meta table
            h2Connection.createStatement().execute(String.format("CREATE TABLE  %s.%s_META (" +
                    "TABLE_NAME VARCHAR(32) NOT NULL," +
                    "ROW_COUNT INTEGER NOT NULL," +
                    "COLUMN_COUNT INTEGER NOT NULL," +
                    "UNIQUE(TABLE_NAME)" +
                    ");", schema, tableName));

            //save data in tables
            try (final PreparedStatement insertValuesStatement = h2Connection.prepareStatement(
                    "INSERT INTO " + schema + "." + tableName + " ("
                            + String.join(", ", columns)
                            + ") VALUES ("
                            + columns.stream().map(c -> "?").collect(Collectors.joining(", "))
                            + ")"
            )) {
                int rowsCount = 0;
                while ((rowLimit == 0 || resultSet.getRow() < rowLimit) && resultSet.next()) {
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        insertValuesStatement.setObject(i, resultSet.getObject(i));
                    }
                    rowsCount++;
                    insertValuesStatement.addBatch();
                }
                insertValuesStatement.executeBatch();

                rowCount = rowCount == -1L ? rowsCount : rowCount;
                resultString = createTable.toString()
                        .replace(("CREATE TABLE " + schema + "." + tableName + " ( \n"),
                                "Table " + tableName + " successfully created in h2 database \nColumns :\n")
                        .replace(");", "") + "\n" + rowCount + " rows affected";
            }

            // clean meta
            h2Connection.createStatement()
                    .execute(String.format("DELETE FROM %s.%s_META WHERE TABLE_NAME = '%s';",
                            schema,
                            tableName,
                            tableName));

            //add meta
            h2Connection.createStatement().execute(String.format("INSERT INTO %s.%s_META  " +
                            "VALUES ('" + tableName.toUpperCase() + "'," + rowCount + ",'" + md.getColumnCount() + "');",
                    schema,
                    tableName));

            return resultString;
        } catch (final Exception ex) {
            return ex.getMessage();
        }
    }

    public String dropTable(final String tblName,
                            final String schema,
                            final Connection connection) throws SQLException {
        try {
            connection.createStatement().execute(String.format("DROP TABLE IF EXISTS %s.%s;", schema, tblName));
            return String.format("Table %s.%s drop syccessfully", schema, tblName);
        } catch (final Exception ex) {
            return ex.getMessage();
        }
    }


    /**
     * Write data into H2/inner_datasource
     */
    private String saveRealTable(final ResultSet resultSet,
                                 final String tableName,
                                 final Connection connection,
                                 final int rowLimit) throws Exception {

        if (StringUtils.isEmpty(tableName)) {
            return null;
        }
        final Statement statement = connection.createStatement();
        statement.execute("DROP TABLE IF EXISTS " + tableName + ";");

        // create schema script text
        final StringBuilder createTable = new StringBuilder();
        createTable.append("CREATE TABLE ").append(tableName).append(" ( \n");
        final ResultSetMetaData md = resultSet.getMetaData();
        for (int i = 1; i < md.getColumnCount() + 1; i++) {
            createTable.append(
                    StringUtils.isNotEmpty(md.getColumnLabel(i))
                            ? md.getColumnLabel(i)
                            : md.getColumnName(i)
            );
            createTable.append(" \t");

            createTable.append(md.getColumnTypeName(i));

            if (i != md.getColumnCount()) {
                createTable.append(", \n");
            }
        }
        createTable.append(");");

        statement.execute(createTable.toString());

        final List<String> columns = new ArrayList<>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
            columns.add(md.getColumnName(i));
        }

        try (final PreparedStatement s2 = connection.prepareStatement(
                "INSERT INTO " + tableName + " ("
                        + String.join(", ", columns)
                        + ") VALUES ("
                        + columns.stream().map(c -> "?").collect(Collectors.joining(", "))
                        + ")"
        )) {
            int rowsCount = 0;
            while ((rowLimit == 0 || resultSet.getRow() < rowLimit) && resultSet.next()) {
                for (int i = 1; i <= md.getColumnCount(); i++) {
                    s2.setObject(i, resultSet.getObject(i));
                }
                rowsCount++;
                s2.addBatch();
            }

            s2.executeBatch();
            return createTable.toString()
                    .replace(("CREATE TABLE " + tableName + " ( \n"),
                            "Table " + tableName + " successfully created/loaded to join\nColumns :\n")
                    .replace(");", "") + "\n" + rowsCount + " rows affected";
        }
    }

    private void saveVirtualTable(final ResultSet resultSet,
                                  final String tableName,
                                  final Connection connection,
                                  final int rowsCount,
                                  final boolean addTableFlg) throws Exception {

        if (StringUtils.isEmpty(tableName)) {
            return;
        }
        final Statement createTableStatement = connection.createStatement();
        createTableStatement.execute("CREATE SCHEMA IF NOT EXISTS V_TABLES; CREATE TABLE IF NOT EXISTS V_TABLES.TABLES (" +
                "TABLE_NAME VARCHAR(32) NOT NULL," +
                "ROWS INTEGER NOT NULL," +
                "COLUMNS VARCHAR(1024) NOT NULL," +
                "UNIQUE(TABLE_NAME)" +
                ");");

        final Statement dropTableIfExists = connection.createStatement();
        dropTableIfExists.execute("DELETE FROM V_TABLES.TABLES WHERE TABLE_NAME = '" + tableName.toUpperCase() + "';");

        if (addTableFlg) {
            final StringBuilder createTable = new StringBuilder();
            final ResultSetMetaData md = resultSet.getMetaData();
            for (int i = 1; i < md.getColumnCount() + 1; i++) {
                createTable.append(
                        StringUtils.isNotEmpty(md.getColumnLabel(i))
                                ? md.getColumnLabel(i)
                                : md.getColumnName(i)
                );
                createTable.append(" \t");

                createTable.append(md.getColumnTypeName(i));

                if (i != md.getColumnCount()) {
                    createTable.append(", \n");
                }
            }

            final Statement addTable = connection.createStatement();
            addTable.execute("INSERT INTO V_TABLES.TABLES  " +
                    "VALUES ('" + tableName.toUpperCase() + "'," + rowsCount + ",'" + createTable.toString().toUpperCase() + "');");
        }
    }


    /**
     * Wraps result set to table.
     *
     * @param resultSet - result set to convert.
     * @return converted result, {@code null} if process failed.
     */
    @Nullable
    public String wrapResults(final ResultSet resultSet) {
        try {
            final ResultSetMetaData md = resultSet.getMetaData();
            final StringBuilder msg = new StringBuilder();

            for (int i = 1; i < md.getColumnCount() + 1; i++) {
                if (i > 1) {
                    msg.append('\t');
                }
                if (md.getColumnLabel(i) != null && !md.getColumnLabel(i).equals("")) {
                    msg.append(replaceReservedChars(md.getColumnLabel(i)));
                } else {
                    msg.append(replaceReservedChars(md.getColumnName(i)));
                }
            }
            msg.append('\n');

            while (resultSet.next()) {
                for (int i = 1; i < md.getColumnCount() + 1; i++) {
                    final Object resultObject;
                    final String resultValue;
                    resultObject = resultSet.getObject(i);
                    if (resultObject == null) {
                        resultValue = "null";
                    } else {
                        resultValue = resultSet.getString(i);
                    }
                    msg.append(replaceReservedChars(resultValue));
                    if (i != md.getColumnCount()) {
                        msg.append('\t');
                    }
                }
                msg.append('\n');
            }
            return msg.toString();
        } catch (final Exception e) {
            // LOGGER.error("Failed to parse result", e);
            return null;
        }
    }

    /**
     * For table response replace Tab and Newline characters from the content.
     */
    private String replaceReservedChars(final String str) {
        if (str == null) {
            return "";
        }
        return str.replace('\t', ' ')
                .replace('\n', ' ');
    }

}
