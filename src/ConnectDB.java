import java.sql.*;

public class ConnectDB {

    static Connection connection = null;
    public static void generate(int num_tuples, double sparsity, int num_attributes) throws SQLException {
            try (Statement statement = connection.createStatement()) {
                statement.executeUpdate("DROP VIEW IF EXISTS TOYBOYBSP_NOTNULL, TOYBOYBSP_NULL");
                statement.execute("DROP TABLE IF EXISTS H");

                statement.execute("CREATE TABLE H (\n oid INT PRIMARY KEY )");
                for(int i = 1; i <= num_tuples; i++){
                    statement.execute("ALTER TABLE H ADD a" + i + " VARCHAR");
                }

                String insertQuery = "INSERT INTO H VALUES (";
                for (int j = 1; j <= num_attributes; j++) {
                    insertQuery += "'" + j + "', ";
                    for (int i = 1; i <= num_tuples; i++) {
                        String value;
                        if (Math.random() < sparsity) {
                            value = "⊥";
                        } else {
                            value = Integer.toString((int) (Math.random() * 100));
                        }
                        insertQuery += "'" + value + "', ";
                    }
                    insertQuery = insertQuery.substring(0, insertQuery.length() - 2);
                    insertQuery += "), (";
                }
                insertQuery = insertQuery.substring(0, insertQuery.length() - 3);
                statement.executeUpdate(insertQuery);

                System.out.println("Created table [H]");

                String generateViews = "CREATE VIEW TOYBOYBSP_NOTNULL AS SELECT * FROM H WHERE";
                for(int i = 1; i < num_tuples; i++ ){
                    generateViews += " a" + i + " != '⊥' AND";
                }
                generateViews += " a" + num_tuples + " != '⊥'";
                statement.executeUpdate(generateViews);

                generateViews = "CREATE VIEW TOYBOYBSP_NULL AS SELECT * FROM H WHERE";
                for(int i = 1; i < num_tuples; i++ ){
                    generateViews += " a" + i + " = '⊥' OR";
                }
                generateViews += " a" + num_tuples + " = '⊥'";
                statement.executeUpdate(generateViews);

                System.out.println("Views erfolgreich erstellt.");
            }
        }

    public static void main(String[] args){
        int num_tuples = 5;
        double sparsity = 0.5;
        int num_attributes = 1000;
        try {
                Class.forName("org.postgresql.Driver");
                connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "alex");
                if(connection != null){
                    System.out.println("Connected");
                    generate(num_tuples, sparsity, num_attributes);

                } else {
                    System.out.println("NOT Connected");
            }

        } catch (Exception e){
            System.out.println(e);
        }
    }

}
