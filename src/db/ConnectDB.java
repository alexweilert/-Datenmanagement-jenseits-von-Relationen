package db;

import java.sql.*;

public class ConnectDB {
    Connection connection;

    public ConnectDB(Connection connection){
            this.connection = connection;
    }

    public void generate(int num_tuples, double sparsity, int num_attributes) throws SQLException {
            try (Statement statement = this.connection.createStatement()) {
                // Delete Tables and Views, if exists.
                statement.executeUpdate("DROP VIEW IF EXISTS NUM_ATTRIBUTES, NUM_TUPLES, SPARSITY, TOY_BSP_NOTNULL, TOY_BSP_NULL");
                statement.execute("DROP TABLE IF EXISTS H");

                // Create Table
                statement.execute("CREATE TABLE H (\n oid INT PRIMARY KEY )");
                for(int i = 1; i <= num_tuples; i++){
                    statement.execute("ALTER TABLE H ADD a" + i + " VARCHAR");
                }
                char alphabet = 'a';
                int number = 1;
                int counter = 0;

                String insertQuery = "INSERT INTO H VALUES (";
                for (int j = 1; j <= num_attributes; j++) {
                    insertQuery += "'" + j + "', ";
                    for (int i = 1; i <= num_tuples; i++) {
                        String value;
                        if (Math.random() < sparsity) {
                            value = "NULL";
                        } else {
                            if (alphabet < '{' && ( counter >= 0 && counter < 5 )) {
                                value = String.valueOf(alphabet);
                                counter++;
                            }
                            else {
                                value = String.valueOf(number);
                                counter++;
                                if (alphabet == '{' && counter == 10){
                                    counter = 5;
                                    number++;
                                } else if(counter == 10){
                                    counter = 0;
                                    number++;
                                    alphabet++;
                                }
                            }
                        }
                        if(value != "NULL") {
                            insertQuery += "'" + value + "', ";
                        }
                        else{
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
                statement.executeUpdate( "CREATE VIEW NUM_ATTRIBUTES AS SELECT count(h) FROM H" );

                // Create View for Rows
                statement.executeUpdate("CREATE VIEW NUM_TUPLES AS SELECT count(*) FROM information_schema.columns WHERE table_name = 'h'");

                // Create View for sparsity
                statement.executeUpdate(generateViewSpar(num_tuples));

            }
    }

    private static String generateViewSpar(int num_tuples) {
        String generateViewSpar = "CREATE VIEW SPARSITY AS SELECT ((ROUND(AVG(SPARSITY), 2)) + 1) AS CHECK_SPARSITY FROM ( ";
        for(int i = 1; i < num_tuples; i++){
            if(i == 1){
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
            for(int i = 1; i < num_tuples; i++ ){
                generateViews += " a" + i + " IS NOT NULL AND";
            }
            generateViews += " a" + num_tuples + " IS NOT NULL";
            statement.executeUpdate(generateViews);

            generateViews = "CREATE VIEW TOY_BSP_NULL AS SELECT * FROM H WHERE";
            for(int i = 1; i < num_tuples; i++ ){
                generateViews += " a" + i + " IS NULL OR";
            }
            generateViews += " a" + num_tuples + " IS NULL";
            statement.executeUpdate(generateViews);
        }
    }
}
