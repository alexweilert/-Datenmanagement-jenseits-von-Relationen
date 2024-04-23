package db;

import java.sql.*;
import java.util.ArrayList;

public class ConnectDB {
    Connection connection;

    public ConnectDB() {
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

    public void generate(int num_attributes, double sparsity, int num_tuples, String create_table, long time) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            // Delete Tables and Views, if exists.
            if(create_table.equals("h")){
                statement.executeUpdate("DROP VIEW IF EXISTS TOY_BSP_NOTNULL, TOY_BSP_NULL");
            }
            statement.executeUpdate("DROP VIEW IF EXISTS NUM_ATTRIBUTES, NUM_TUPLES, SPARSITY");
            statement.execute("DROP TABLE IF EXISTS " + create_table);

            long start_time = System.currentTimeMillis();
            long max_time = System.currentTimeMillis() + (time * 1000);
            long counter_querry = 0;
            boolean time_over = false;
            // Create Table
            statement.execute("CREATE TABLE " + create_table + " (\n oid INT PRIMARY KEY)");
            for (int i = 1; i <= num_attributes; i++) {
                statement.execute("ALTER TABLE " + create_table + " ADD a" + i + " VARCHAR");
            }
            char alphabet = 'a';
            int number = 1;
            int counter_integer = 0;
            int counter_string = 0;

            StringBuilder insertQuery = new StringBuilder("INSERT INTO " + create_table + " VALUES (");
            for (int j = 1; j <= num_tuples; j++) {
                if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                    time_over = true;
                    System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                }
                insertQuery.append("'").append(j).append("', ");
                int string_int = 0;
                for (int i = 1; i <= num_attributes; i++) {
                    if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                        time_over = true;
                        System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                    }
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
                        insertQuery.append("'").append(value).append("', ");
                    } else {
                        insertQuery.append(value).append(", ");
                    }
                }
                insertQuery = new StringBuilder(insertQuery.substring(0, insertQuery.length() - 2));
                insertQuery.append("), (");
                counter_querry++;
                if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                    time_over = true;
                    System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                }
            }
            insertQuery = new StringBuilder(insertQuery.substring(0, insertQuery.length() - 3));
            statement.executeUpdate(insertQuery.toString());
            if (!time_over) {
                System.out.println(((System.currentTimeMillis() - start_time)/1000) + " seconds with a total of " + counter_querry + " querryies");
            }
            generateViews(num_attributes, create_table);
        }
    }

    private void generateViews(int num_attributes, String create_table) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
        // Create View for Columns
        statement.executeUpdate("CREATE VIEW NUM_ATTRIBUTES AS SELECT count(*) FROM " + create_table);

        // Create View for Rows
        statement.executeUpdate("CREATE VIEW NUM_TUPLES AS SELECT count(*) FROM information_schema.columns WHERE table_name = '" + create_table + "'");

        // Create Views for sparsity
        StringBuilder generateViewSpar = new StringBuilder("CREATE VIEW SPARSITY AS SELECT ((ROUND(AVG(SPARSITY), 2)) + 1) AS CHECK_SPARSITY FROM ( ");
        for (int i = 1; i < num_attributes; i++) {
            if (i == 1) {
                generateViewSpar.append("\n SELECT (1.0 - COUNT(a").append(i).append("))/ COUNT(*) AS SPARSITY FROM ").append(create_table).append(" UNION ALL");
            } else {
                generateViewSpar.append("\n SELECT (1.0 - COUNT(a").append(i).append("))/ COUNT(*) FROM ").append(create_table).append(" UNION ALL");
            }
        }
        generateViewSpar.append("\n SELECT (1.0 - COUNT(a").append(num_attributes).append("))/ COUNT(*) FROM ").append(create_table).append(" ) AS SINGLE_SPARSITY");
        statement.execute(generateViewSpar.toString());
        }
    }

    public void generateToyBsp(int num_attributes, String select_table) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            // Create Toy example out of table H
            StringBuilder generateViews = new StringBuilder("CREATE VIEW TOY_BSP_NOTNULL AS SELECT oid, \n");
            for (int i = 1; i < num_attributes; i++) {
                if (i % 2 == 1) {
                    generateViews.append("CASE WHEN a" + i + " IS NULL THEN NULL ELSE CAST(a" + i + " AS VARCHAR) END as a" + i + ", \n");
                } else {
                    generateViews.append("CASE WHEN a" + i + " IS NULL THEN NULL ELSE CAST(a" + i + " AS INT) END as a" + i + ", \n");
                }
            }
            if (num_attributes % 2 == 1) {
                generateViews.append("CASE WHEN a" + num_attributes + " IS NULL THEN NULL ELSE CAST(a" + num_attributes + " AS VARCHAR) END as a" + num_attributes + " \n");
            } else {
                generateViews.append("CASE WHEN a" + num_attributes + " IS NULL THEN NULL ELSE CAST(a" + num_attributes + " AS INT) END as a" + num_attributes + " \n");
            }
            generateViews.append("FROM " + select_table + " WHERE \n");

            for (int i = 1; i < num_attributes; i++) {
                generateViews.append(" a").append(i).append(" IS NOT NULL AND");
            }
            generateViews.append(" a").append(num_attributes).append(" IS NOT NULL");
            statement.executeUpdate(generateViews.toString());



            generateViews = new StringBuilder("CREATE VIEW TOY_BSP_NULL AS SELECT oid, \n");
            for (int i = 1; i < num_attributes; i++) {
                if (i % 2 == 1) {
                    generateViews.append("CASE WHEN a" + i + " IS NULL THEN NULL ELSE CAST(a" + i + " AS VARCHAR) END as a" + i + ", \n");
                } else {
                    generateViews.append("CASE WHEN a" + i + " IS NULL THEN NULL ELSE CAST(a" + i + " AS INT) END as a" + i + ", \n");
                }
            }
            if (num_attributes % 2 == 1) {
                generateViews.append("CASE WHEN a" + num_attributes + " IS NULL THEN NULL ELSE CAST(a" + num_attributes + " AS VARCHAR) END as a" + num_attributes + " \n");
            } else {
                generateViews.append("CASE WHEN a" + num_attributes + " IS NULL THEN NULL ELSE CAST(a" + num_attributes + " AS INT) END as a" + num_attributes + " \n");
            }
            generateViews.append("FROM " + select_table + " WHERE \n");



            for (int i = 1; i < num_attributes; i++) {
                generateViews.append(" a").append(i).append(" IS NULL OR");
            }
            generateViews.append(" a").append(num_attributes).append(" IS NULL");
            statement.executeUpdate(generateViews.toString());
        }
    }


    public void h2v(String select_table_name, String create_table, String create_v2h_view, long time) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP VIEW IF EXISTS " + create_v2h_view);
            statement.execute("DROP MATERIALIZED VIEW IF EXISTS mv_" + create_table + ", mv_" + create_table + "_string, mv_" + create_table + "_integer");
            statement.execute("DROP TABLE IF EXISTS " + create_table + ", " + create_table +"_string, " + create_table + "_integer");
            statement.execute("DROP INDEX IF EXISTS idx_key_" + create_table + ", idx_key_" + create_table + "_string, idx_key_" + create_table + "_integer");

            statement.execute("CREATE TABLE " + create_table + "_string (\n oid INT, key varchar, val varchar)");
            statement.execute("CREATE TABLE " + create_table + "_integer (\n oid INT, key varchar, val INT)");

            long start_time = System.currentTimeMillis();
            long max_time = System.currentTimeMillis() + time * 1000;
            long counter_querry = 0;
            boolean time_over = false;

            ResultSet rs = statement.executeQuery("SELECT * FROM " + select_table_name);
            ResultSetMetaData rsmd = rs.getMetaData();
            StringBuilder sql_string = new StringBuilder("INSERT INTO " + create_table + "_string (oid, key, val) VALUES ");
            StringBuilder sql_integer = new StringBuilder("INSERT INTO " + create_table + "_integer (oid, key, val) VALUES ");
            String id = "";
            while (rs.next()) {
                int count = 1;
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                        time_over = true;
                        System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                    }
                    if (rsmd.getColumnName(i).equals("oid")) {
                        id = rs.getString(i);
                    } else {
                        if (rs.getString(i) == null) {
                            count++;
                            if (rsmd.getColumnCount() == count) {
                                sql_string.append("( '").append(id).append("', '").append(rsmd.getColumnName(i)).append("', ").append(rs.getString(i)).append(" ), ");
                                counter_querry++;
                                if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                                    time_over = true;
                                    System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                                }
                            }
                        } else {
                            if (i % 2 == 0) {
                                sql_string.append("('").append(id).append("', '").append(rsmd.getColumnName(i)).append("', '").append(rs.getString(i)).append("'), ");
                            } else {
                                sql_integer.append("('").append(id).append("', '").append(rsmd.getColumnName(i)).append("', '").append(rs.getString(i)).append("'), ");
                            }
                            counter_querry++;
                            if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                                time_over = true;
                                System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                            }
                        }
                    }
                }
            }
            sql_string = new StringBuilder(sql_string.substring(0, sql_string.length() - 2));
            statement.executeUpdate(sql_string.toString());
            sql_integer = new StringBuilder(sql_integer.substring(0, sql_integer.length() - 2));
            statement.executeUpdate(sql_integer.toString());

            if (!time_over) {
                System.out.println(((System.currentTimeMillis() - start_time)/1000) + " seconds with a total of " + counter_querry + " querryies");
            }

            statement.execute("CREATE INDEX idx_key_" + create_table + "_string ON " + create_table + "_string (oid)");
            statement.execute("CREATE INDEX idx_key_" + create_table + "_integer ON " + create_table + "_integer (oid)");

            statement.execute("CREATE MATERIALIZED VIEW mv_" + create_table + "_string AS SELECT * FROM " + create_table + "_string WHERE key = 'a1'");
            statement.execute("CREATE MATERIALIZED VIEW mv_" + create_table + "_integer AS SELECT * FROM " + create_table + "_integer WHERE key = 'a2'");
        }
    }


    public void v2h(String select_table, String create_table, long time) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP VIEW IF EXISTS " + create_table);
            statement.execute("DROP MATERIALIZED VIEW IF EXISTS mv_" + create_table + ", mv_" + create_table + "_string, mv_" + create_table + "_integer");
            ArrayList<String> keys = new ArrayList<>();

            keys = getKeys(select_table);

            StringBuilder sql = new StringBuilder("CREATE VIEW " + create_table + " AS SELECT COALESCE(s.oid, i.oid) AS oid,");
            for (int i = 0; i < keys.size(); i++) {
                if (i % 2 == 0) { // String
                    sql.append("\n MAX(CASE WHEN s.key = '" + keys.get(i) + "' THEN s.val END) AS a" + (i+1) + ", ");
                } else {          // Integer
                    sql.append("\n MAX(CASE WHEN i.key = '" + keys.get(i) + "' THEN i.val END) AS a" + (i+1) + ", ");
                }
            }

            sql = new StringBuilder(sql.substring(0, sql.length() - 2));
            sql.append("\n FROM " + select_table + "_string s \n" +
                    "FULL OUTER JOIN " + select_table + "_integer i ON s.oid = i.oid \n" +
                    "GROUP BY COALESCE(s.oid, i.oid) ORDER BY COALESCE(s.oid, i.oid)");
            statement.executeUpdate(sql.toString());

            statement.execute("CREATE MATERIALIZED VIEW mv_" + create_table + " AS SELECT * FROM " + create_table + " WHERE a2 = '1'");
        }
    }

    public ArrayList<String> getKeys(String select_table) throws SQLException{
        try (Statement statement = this.connection.createStatement()) {
            ArrayList<String> keys = new ArrayList<>();

            ResultSet rs = statement.executeQuery("SELECT * FROM " + select_table + "_string");
            ResultSetMetaData rsmd = rs.getMetaData();

            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (rsmd.getColumnName(i).equals("key") && !keys.contains(rs.getString(i))) {
                        keys.add(rs.getString(i));
                    }
                }
            }

            rs = statement.executeQuery("SELECT * FROM " + select_table +"_integer");
            rsmd = rs.getMetaData();

            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if (rsmd.getColumnName(i).equals("key") && !keys.contains(rs.getString(i))) {
                        keys.add(rs.getString(i));
                    }
                }
            }

            keys.sort((o1, o2) -> o1.compareTo(o2));
            return keys;
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

            while (rs.next()) {
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
