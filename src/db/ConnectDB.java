package db;

import java.sql.*;
import java.util.ArrayList;

public class ConnectDB {
    Connection connection;


    public ConnectDB(Connection connection) {
        this.connection = connection;
    }


    public void generate(int num_tuples, double sparsity, int num_attributes) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            // Delete Tables and Views, if exists.
            statement.executeUpdate("DROP VIEW IF EXISTS NUM_ATTRIBUTES, NUM_TUPLES, SPARSITY, TOY_BSP_NOTNULL, TOY_BSP_NULL");
            statement.execute("DROP TABLE IF EXISTS H, H2V, V2H");

            // Create Table
            statement.execute("CREATE TABLE H (\n oid INT PRIMARY KEY )");
            for (int i = 1; i <= num_tuples; i++) {
                statement.execute("ALTER TABLE H ADD a" + i + " VARCHAR");
            }
            char alphabet = 'a';
            char alphabet2 = ' ';
            char alphabet3 = ' ';
            int number = 1;
            int counter_integer = 0;
            int counter_string = 0;

            String insertQuery = "INSERT INTO H VALUES (";
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
                            if (alphabet == '{' && alphabet2 == 'z' && alphabet3 == ' ') {
                                alphabet3 = 'a';
                                alphabet2 = 'a';
                                alphabet = 'a';
                            } else if (alphabet == '{' && alphabet2 == ' ') {
                                alphabet2 = 'a';
                                alphabet = 'a';
                            } else if (alphabet == '{' && alphabet2 == 'z' && alphabet3 >= 'a') {
                                alphabet3++;
                                alphabet2 = 'a';
                                alphabet = 'a';
                            } else if (alphabet == '{' && alphabet2 <= 'z') {
                                alphabet2++;
                                alphabet = 'a';
                            }

                            if (alphabet3 != ' ')
                                value = String.valueOf(alphabet3) + String.valueOf(alphabet2) + String.valueOf(alphabet);
                            else if (alphabet2 != ' ')
                                value = String.valueOf(alphabet2) + String.valueOf(alphabet);
                            else
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
            statement.executeUpdate("CREATE VIEW NUM_ATTRIBUTES AS SELECT count(h) FROM H");

            // Create View for Rows
            statement.executeUpdate("CREATE VIEW NUM_TUPLES AS SELECT count(*) FROM information_schema.columns WHERE table_name = 'h'");

            // Create View for sparsity
            statement.executeUpdate(generateViewSpar(num_tuples));
        }
    }


    private static String generateViewSpar(int num_tuples) {
        String generateViewSpar = "CREATE VIEW SPARSITY AS SELECT ((ROUND(AVG(SPARSITY), 2)) + 1) AS CHECK_SPARSITY FROM ( ";
        for (int i = 1; i < num_tuples; i++) {
            if (i == 1) {
                generateViewSpar += "\n SELECT (1.0 - COUNT(a" + i + "))/ COUNT(*) AS SPARSITY FROM h UNION ALL";
            } else {
                generateViewSpar += "\n SELECT (1.0 - COUNT(a" + i + "))/ COUNT(*) FROM h UNION ALL";
            }
        }
        generateViewSpar += "\n SELECT (1.0 - COUNT(a" + num_tuples + "))/ COUNT(*) FROM h ) AS SINGLE_SPARSITY";
        return generateViewSpar;
    }


    public boolean closeConnection(Connection connection) throws SQLException {
        connection.close();
        return connection.isClosed();
    }


    public void generateToyBsp(int num_tuples) throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            // Create Toy example out of table H
            String generateViews = "CREATE VIEW TOY_BSP_NOTNULL AS SELECT * FROM H WHERE";
            for (int i = 1; i < num_tuples; i++) {
                generateViews += " a" + i + " IS NOT NULL AND";
            }
            generateViews += " a" + num_tuples + " IS NOT NULL";
            statement.executeUpdate(generateViews);

            generateViews = "CREATE VIEW TOY_BSP_NULL AS SELECT * FROM H WHERE";
            for (int i = 1; i < num_tuples; i++) {
                generateViews += " a" + i + " IS NULL OR";
            }
            generateViews += " a" + num_tuples + " IS NULL";
            statement.executeUpdate(generateViews);
        }
    }


    public void h2v() throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS H2V");
            statement.execute("CREATE TABLE H2V (\n oid varchar, key varchar, val varchar)");

            ResultSet rs = statement.executeQuery("SELECT * FROM toy_bsp_null");
            ResultSetMetaData rsmd = rs.getMetaData();

            String sql = "INSERT INTO h2v (oid, key, val) VALUES ";
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
                                    sql += "( '" + id + "', '" + rsmd.getColumnName(j) + "', '" + rs.getString(j) + "' ), ";
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

    public void v2h() throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS V2H");

            ResultSet rs = statement.executeQuery("SELECT * FROM H2V");
            ResultSetMetaData rsmd = rs.getMetaData();

            String sql = "CREATE TABLE V2H ( \n oid VARCHAR, ";
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
            for( int i = 0; i < keys.size(); i++ ) {
                sql +=  keys.get(i) + " VARCHAR, ";
                attr += keys.get(i) + ", ";
            }
            sql = sql.substring(0, sql.length() - 2);
            sql += ")";
            statement.executeUpdate(sql);

            attr = attr.substring(0, attr.length() - 2);
            sql = "INSERT INTO V2H VALUES( oid, " + attr + " )";
            rs = statement.executeQuery("SELECT * FROM H2V");
            while(rs.next()){
                for(int i = 1; i <= rsmd.getColumnCount(); i++) {
                    System.out.println(rsmd.getColumnName(i) + " " + rs.getString(i));
                }
            }
        }
    }
}
