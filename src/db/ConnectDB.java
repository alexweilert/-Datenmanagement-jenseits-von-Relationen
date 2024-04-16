package db;

import java.sql.*;
import java.util.ArrayList;

public class ConnectDB {
    Connection connection;

    public ConnectDB() throws SQLException, ClassNotFoundException {
    }

    public boolean openConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        this.connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "dragi");
        return !isConnectionClosed();
    }

    public boolean isConnectionClosed() throws SQLException {
        return this.connection.isClosed();
    }

    public void closeConnection() throws SQLException {
        this.connection.close();

    }

    public void generate(int num_tuples, double sparsity, int num_attributes, String create_table) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            // Delete Tables and Views, if exists.
            if(create_table.equals("h")){
                statement.executeUpdate("DROP VIEW IF EXISTS TOY_BSP_NOTNULL, TOY_BSP_NULL");
            }
            statement.executeUpdate("DROP VIEW IF EXISTS NUM_ATTRIBUTES, NUM_TUPLES, SPARSITY");
            statement.execute("DROP TABLE IF EXISTS " + create_table);

            // Create Table
            statement.execute("CREATE TABLE " + create_table + " (\n oid INT PRIMARY KEY )");
            for (int i = 1; i <= num_tuples; i++) {
                statement.execute("ALTER TABLE " + create_table + " ADD a" + i + " VARCHAR");
            }
            char alphabet = 'a';
            int number = 1;
            int counter_integer = 0;
            int counter_string = 0;

            String insertQuery = "INSERT INTO " + create_table + " VALUES (";
            for (int j = 1; j <= num_attributes; j++) {
                insertQuery += "'" + j + "', ";
                int string_int = 0;
                for (int i = 1; i <= num_tuples; i++) {
                    String value;
                    if (Math.random() < sparsity) {
                        value = "NULL";
                    } else {
                        if (string_int % 2 == 0) {
                            if (counter_string >= 5) {
                                counter_string = 0;
                                alphabet++;
                            }
                            value = String.valueOf(alphabet);
                            counter_string++;

                        } else {
                            value = String.valueOf(number);
                            counter_integer++;

                            if (counter_integer >= 5) {
                                counter_integer = 0;
                                number++;
                            }
                        }
                    }

                    string_int++;
                    if (!value.equals("NULL")) {
                        insertQuery += "'" + value + "', ";
                    } else {
                        insertQuery += value + ", ";
                    }
                }
                insertQuery = insertQuery.substring(0, insertQuery.length() - 2);
                insertQuery += "), (";

            }
            insertQuery = insertQuery.substring(0, insertQuery.length() - 3);
            statement.executeUpdate(insertQuery);
            // Table created

            // Create View for Columns
            statement.executeUpdate("CREATE VIEW NUM_ATTRIBUTES AS SELECT count(*) FROM " + create_table);

            // Create View for Rows
            statement.executeUpdate("CREATE VIEW NUM_TUPLES AS SELECT count(*) FROM information_schema.columns WHERE table_name = '" + create_table + "'");


            // Create View for sparsity
            statement.executeUpdate(generateViewSpar(num_tuples, create_table));
        }
    }

    private static String generateViewSpar(int num_tuples, String create_table) {
        String generateViewSpar = "CREATE VIEW SPARSITY AS SELECT ((ROUND(AVG(SPARSITY), 2)) + 1) AS CHECK_SPARSITY FROM ( ";
        for (int i = 1; i < num_tuples; i++) {
            if (i == 1) {
                generateViewSpar += "\n SELECT (1.0 - COUNT(a" + i + "))/ COUNT(*) AS SPARSITY FROM " + create_table + " UNION ALL";
            } else {
                generateViewSpar += "\n SELECT (1.0 - COUNT(a" + i + "))/ COUNT(*) FROM " + create_table + " UNION ALL";
            }
        }
        generateViewSpar += "\n SELECT (1.0 - COUNT(a" + num_tuples + "))/ COUNT(*) FROM " + create_table + " ) AS SINGLE_SPARSITY";
        return generateViewSpar;
    }

    public void generateToyBsp(int num_tuples, String select_table) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            // Create Toy example out of table H
            String generateViews = "CREATE VIEW TOY_BSP_NOTNULL AS SELECT * FROM " + select_table + " WHERE";
            for (int i = 1; i < num_tuples; i++) {
                generateViews += " a" + i + " IS NOT NULL AND";
            }
            generateViews += " a" + num_tuples + " IS NOT NULL";
            statement.executeUpdate(generateViews);

            generateViews = "CREATE VIEW TOY_BSP_NULL AS SELECT * FROM " + select_table + " WHERE";
            for (int i = 1; i < num_tuples; i++) {
                generateViews += " a" + i + " IS NULL OR";
            }
            generateViews += " a" + num_tuples + " IS NULL";
            statement.executeUpdate(generateViews);
        }
    }

    public void h2v(String select_table_name, String create_table) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP MATERIALIZED VIEW IF EXISTS mv_v");
            statement.execute("DROP TABLE IF EXISTS " + create_table);
            statement.execute("CREATE TABLE " + create_table + " (\n oid varchar, key varchar, val varchar)");

            statement.execute("DROP INDEX IF EXISTS idx_key");
            statement.execute("CREATE INDEX idx_key ON V (oid)");
            //System.out.println( statement.execute("CREATE INDEX idx_key ON V (oid)"));


            String materi = "CREATE MATERIALIZED VIEW mv_v AS SELECT * FROM V WHERE key = 'a1'";
            statement.execute(materi);
            System.out.println(materi);
            // System.out.println( statement.execute("CREATE MATERIALIZED VIEW mv_v AS SELECT * FROM V WHERE key = 'a1'"));

            ResultSet rs = statement.executeQuery("SELECT * FROM " + select_table_name);
            ResultSetMetaData rsmd = rs.getMetaData();

            String sql = "INSERT INTO " + create_table + " (oid, key, val) VALUES ";
            String id = "";
            while (rs.next()) {
                int count = 1;
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (rsmd.getColumnName(i).equals("oid")) {
                        id = rs.getString(i);
                    } else {
                        if (rs.getString(i) == null) {
                            count++;
                            if (rsmd.getColumnCount() == count) {
                                for (int j = 2; j <= rsmd.getColumnCount(); j++) {
                                    sql += "( '" + id + "', '" + rsmd.getColumnName(j) + "', " + rs.getString(j) + " ), ";
                                }
                            }
                        } else {
                            sql += "('" + id + "', '" + rsmd.getColumnName(i) + "', '" + rs.getString(i) + "'), ";
                        }
                    }
                }
            }
            sql = sql.substring(0, sql.length() - 2);
            statement.executeUpdate(sql);
        }
    }

    public void v2h(String select_table, String create_table) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + create_table);

            ResultSet rs = statement.executeQuery("SELECT * FROM " + select_table);
            ResultSetMetaData rsmd = rs.getMetaData();
            // Create Table V2H
            String sql = "CREATE TABLE " + create_table + " ( \n oid VARCHAR, ";
            String attr = "";
            ArrayList<String> keys = new ArrayList<>();

            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (rsmd.getColumnName(i).equals("key") && !keys.contains(rs.getString(i))) {
                        keys.add(rs.getString(i));
                    }
                }
            }

            keys.sort((o1, o2) -> o1.compareTo(o2));
            for (int i = 0; i < keys.size(); i++) {
                sql += keys.get(i) + " VARCHAR, ";
                attr += keys.get(i) + ", ";
            }
            sql = sql.substring(0, sql.length() - 2);
            sql += ")";
            // Generate Table V2H out of String.
            statement.executeUpdate(sql);


            attr = attr.substring(0, attr.length() - 2);
            sql = "INSERT INTO " + create_table + " ( oid, " + attr + " ) VALUES ";

            rs = statement.executeQuery("SELECT * FROM " + select_table);

            String oid = "";
            String old_oid = "";
            String key = "";
            int key_index = 0;
            int counter = 0;
            String val = "";
            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (rsmd.getColumnName(i).equals("oid")) {
                        oid = rs.getString(i);
                    } else if (rsmd.getColumnName(i).equals("key")) {
                        key = rs.getString(i);
                        key_index = Integer.valueOf(key.substring(1));
                    } else if (rsmd.getColumnName(i).equals("val")) {
                        val = rs.getString(i);
                    }
                }
                if (oid.equals(old_oid)) {
                    if (counter < key_index - 1) {
                        while (counter < key_index - 1) {
                            sql += ", NULL";
                            counter++;
                        }
                    }
                    if (val != null) {
                        sql += ", '" + val + "'";
                    } else {
                        sql += ", NULL";
                    }
                    counter++;
                } else {
                    if (counter >= keys.size()) {
                        sql += " ), ";
                        counter = 0;
                    } else if (!old_oid.isEmpty()) {
                        for (int j = counter; j < keys.size(); j++) {
                            sql += ", NULL";
                            counter++;
                        }
                        sql += " ), ";
                        counter = 0;
                    }

                    sql += "( " + oid;
                    old_oid = oid;
                    if (counter < key_index) {
                        for (int i = 1; i < key_index; i++) {
                            sql += ", NULL";
                            counter++;
                        }
                    }
                    if (val != null) {
                        sql += ", '" + val + "'";
                    } else {
                        sql += ", NULL";
                    }
                    counter++;
                }
            }
            while (counter < keys.size()) {
                sql += ", NULL";
                counter++;
            }
            sql += " )";
            statement.execute(sql);
        }
    }

    public void printStorageSize(String tableName) {
        try (Statement statement = this.connection.createStatement()) {
            String sql = "SELECT " +
                    "pg_size_pretty(pg_total_relation_size('\"' || table_schema || '\".\"' || table_name || '\"')) AS total_size, " +
                    "pg_size_pretty(pg_indexes_size('\"' || table_schema || '\".\"' || table_name || '\"')) AS indexes_size " +
                    "FROM information_schema.tables " +
                    "WHERE table_name = '" + tableName + "';";

            ResultSet rs = statement.executeQuery(sql);

            if (rs.next()) {
                String totalSize = rs.getString("total_size");
                String indexesSize = rs.getString("indexes_size");
                System.out.println("Total Size of " + tableName + ": " + totalSize);
                System.out.println("Indexes Size of " + tableName + ": " + indexesSize);
            }

            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
            System.out.println("Error while calculating storage size of " + tableName);
        }
    }

}
