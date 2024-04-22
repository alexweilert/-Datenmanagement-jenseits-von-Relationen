package db;

import java.sql.*;
import java.util.ArrayList;

public class ConnectDB {
    Connection connection;

    public ConnectDB() {
    }

    public boolean openConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        this.connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "alex");
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
            // Table created

            // Create View for Columns
            statement.executeUpdate("CREATE VIEW NUM_ATTRIBUTES AS SELECT count(*) FROM " + create_table);

            // Create View for Rows
            statement.executeUpdate("CREATE VIEW NUM_TUPLES AS SELECT count(*) FROM information_schema.columns WHERE table_name = '" + create_table + "'");


            // Create Views for sparsity
            statement.executeUpdate(generateViewSpar(num_attributes, create_table));
        }
    }

    private static String generateViewSpar(int num_attributes, String create_table) {
        StringBuilder generateViewSpar = new StringBuilder("CREATE VIEW SPARSITY AS SELECT ((ROUND(AVG(SPARSITY), 2)) + 1) AS CHECK_SPARSITY FROM ( ");
        for (int i = 1; i < num_attributes; i++) {
            if (i == 1) {
                generateViewSpar.append("\n SELECT (1.0 - COUNT(a").append(i).append("))/ COUNT(*) AS SPARSITY FROM ").append(create_table).append(" UNION ALL");
            } else {
                generateViewSpar.append("\n SELECT (1.0 - COUNT(a").append(i).append("))/ COUNT(*) FROM ").append(create_table).append(" UNION ALL");
            }
        }
        generateViewSpar.append("\n SELECT (1.0 - COUNT(a").append(num_attributes).append("))/ COUNT(*) FROM ").append(create_table).append(" ) AS SINGLE_SPARSITY");
        return generateViewSpar.toString();
    }

    public void generateToyBsp(int num_attributes, String select_table) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            // Create Toy example out of table H
            StringBuilder generateViews = new StringBuilder("CREATE VIEW TOY_BSP_NOTNULL AS SELECT * FROM " + select_table + " WHERE");
            for (int i = 1; i < num_attributes; i++) {
                generateViews.append(" a").append(i).append(" IS NOT NULL AND");
            }
            generateViews.append(" a").append(num_attributes).append(" IS NOT NULL");
            statement.executeUpdate(generateViews.toString());

            generateViews = new StringBuilder("CREATE VIEW TOY_BSP_NULL AS SELECT * FROM " + select_table + " WHERE");
            for (int i = 1; i < num_attributes; i++) {
                generateViews.append(" a").append(i).append(" IS NULL OR");
            }
            generateViews.append(" a").append(num_attributes).append(" IS NULL");
            statement.executeUpdate(generateViews.toString());
        }
    }



    public void h2v(String select_table_name, String create_table, long time) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP MATERIALIZED VIEW IF EXISTS mv_"+create_table);
            statement.execute("DROP TABLE IF EXISTS " + create_table);


            statement.execute("CREATE TABLE " + create_table + " (\n oid INT, key varchar, val varchar)");

            statement.execute("DROP INDEX IF EXISTS idx_key_" + create_table);
            long start_time = System.currentTimeMillis();
            long max_time = System.currentTimeMillis() + time * 1000;
            long counter_querry = 0;
            boolean time_over = false;

            statement.execute("CREATE INDEX idx_key_" + create_table + " ON " + create_table + " (oid)");

            statement.execute("CREATE MATERIALIZED VIEW mv_" + create_table + " AS SELECT * FROM " + create_table + " WHERE key = 'a1'");
            ResultSet rs = statement.executeQuery("SELECT * FROM " + select_table_name);
            ResultSetMetaData rsmd = rs.getMetaData();
            StringBuilder sql = new StringBuilder("INSERT INTO " + create_table + " (oid, key, val) VALUES ");
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
                                sql.append("( '").append(id).append("', '").append(rsmd.getColumnName(i)).append("', ").append(rs.getString(i)).append(" ), ");
                                counter_querry++;
                                if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                                    time_over = true;
                                    System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                                }
                            }
                        } else {
                            sql.append("('").append(id).append("', '").append(rsmd.getColumnName(i)).append("', '").append(rs.getString(i)).append("'), ");
                            counter_querry++;
                            if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                                time_over = true;
                                System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                            }
                        }
                    }
                }
            }
            sql = new StringBuilder(sql.substring(0, sql.length() - 2));
            statement.executeUpdate(sql.toString());
            if (!time_over) {
                System.out.println(((System.currentTimeMillis() - start_time)/1000) + " seconds with a total of " + counter_querry + " querryies");
            }
        }
    }

    public void v2h(String select_table, String create_table, long time) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + create_table);

            ResultSet rs = statement.executeQuery("SELECT * FROM " + select_table);
            ResultSetMetaData rsmd = rs.getMetaData();

            // Create Table V2H
            StringBuilder sql = new StringBuilder("CREATE TABLE " + create_table + " ( \n oid INT PRIMARY KEY, ");
            StringBuilder attr = new StringBuilder();
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
                sql.append(keys.get(i)).append(" VARCHAR, ");
                attr.append(keys.get(i)).append(", ");
            }
            sql = new StringBuilder(sql.substring(0, sql.length() - 2));
            sql.append(")");
            // Generate Table V2H out of String.
            statement.executeUpdate(sql.toString());

            statement.execute("DROP INDEX IF EXISTS idx_key_" + create_table);

            long start_time = System.currentTimeMillis();
            long max_time = System.currentTimeMillis() + (time * 1000);
            long counter_querry = 0;
            boolean time_over = false;

            statement.execute("CREATE INDEX idx_key_" + create_table + " ON " + create_table + " (oid)");

            attr = new StringBuilder(attr.substring(0, attr.length() - 2));
            sql = new StringBuilder("INSERT INTO " + create_table + " ( oid, " + attr + " ) VALUES ");

            rs = statement.executeQuery("SELECT * FROM " + select_table + " ORDER BY oid ASC");

            String oid = "";
            String old_oid = "";
            String key = "";
            int key_index = 0;
            int counter = 0;
            String val = "";

            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                        time_over = true;
                        System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                    }
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
                            sql.append(", NULL");
                            counter++;
                        }
                    }
                    if (val != null) {
                        sql.append(", '").append(val).append("'");
                    } else {
                        sql.append(", NULL");
                    }
                    counter++;
                } else {
                    if (counter >= keys.size()) {
                        sql.append(" ), ");
                        counter = 0;
                        counter_querry++;
                        if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                            time_over = true;
                            System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                        }
                    } else if (!old_oid.isEmpty()) {
                        for (int j = counter; j < keys.size(); j++) {
                            if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                                time_over = true;
                                System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                            }
                            sql.append(", NULL");
                            counter++;
                        }
                        sql.append(" ), ");
                        if(System.currentTimeMillis() >= max_time-200 && System.currentTimeMillis() <= max_time+200 && !time_over){
                            time_over = true;
                            System.out.println("In " + time + " seconds we did generate " + counter_querry + " querrys");
                        }
                        counter = 0;
                        counter_querry++;
                    }

                    sql.append("( ").append(oid);
                    old_oid = oid;
                    if (counter < key_index) {
                        for (int i = 1; i < key_index; i++) {
                            sql.append(", NULL");
                            counter++;
                        }
                    }
                    if (val != null) {
                        sql.append(", '").append(val).append("'");
                    } else {
                        sql.append(", NULL");
                    }
                    counter++;
                }
            }
            while (counter < keys.size()) {
                sql.append(", NULL");
                counter++;
            }
            sql.append(" )");
            statement.execute(sql.toString());
            counter_querry++;
            if (!time_over) {
                System.out.println(((System.currentTimeMillis() - start_time)/1000) + " seconds with a total of " + counter_querry + " querryies");
            }
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
