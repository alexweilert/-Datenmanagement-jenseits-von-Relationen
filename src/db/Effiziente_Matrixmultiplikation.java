package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class Effiziente_Matrixmultiplikation {
    Connection connection;

    public Effiziente_Matrixmultiplikation() throws SQLException, ClassNotFoundException {
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

    public int[][][] generate(int l, double sparsity) {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP VIEW IF EXISTS C");
            statement.execute("DROP TABLE IF EXISTS A, B");
            // Create Table
            statement.execute("CREATE TABLE A (i INT, j INT , val INT, PRIMARY KEY (i, j))");
            statement.execute("CREATE TABLE B (i INT, j INT , val INT, PRIMARY KEY (i, j))");

            int[][][] matrix = new int[2][][];
            int[][] matrixA = new int[l-1][l];
            int[][] matrixB = new int[l][l-1];
            
            //System.out.println("--- Matrix A ---");
            matrix[0] = generateMatrix(matrixA, sparsity);
            //System.out.println("--- Matrix B ---");
            matrix[1] = generateMatrix(matrixB, sparsity);

            insertMatrix("A", matrix[0]);
            insertMatrix("B", matrix[1]);

            return matrix;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int[][] generateMatrix(int[][] matrix, double sparsity) {
        Random random = new Random();
        for (int i = 0; i < matrix.length; i++) {
            for (int j = 0; j < matrix[0].length; j++) {
                if (random.nextDouble() > sparsity) {
                    matrix[i][j] = random.nextInt(1, 11); // Random value between 0 and 10
                } else {
                    matrix[i][j] = 0;
                }
                //System.out.print(matrix[i][j] + " ");
            }
            //System.out.println();
        }
        return matrix;
    }

    public void insertMatrix(String tableName, int[][] matrix) {
        try (Statement statement = this.connection.createStatement()) {
            StringBuilder insertQuery = new StringBuilder("INSERT INTO " + tableName + " VALUES ");
            for (int i = 0; i < matrix.length; i++) {
                for (int j = 0; j < matrix[i].length; j++) {
                    if (matrix[i][j] != 0) {
                        insertQuery.append("(").append(i + 1).append(",").append(j + 1).append(",").append(matrix[i][j]).append("),");
                    }
                }
            }
            insertQuery.deleteCharAt(insertQuery.length() - 1);
            statement.executeUpdate(insertQuery.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void ansatz0(int[][][] matrix) {
        try (Statement statement = this.connection.createStatement()) {
            int[][] matrixA = matrix[0];
            int[][] matrixB = matrix[1];
            statement.execute("DROP TABLE IF EXISTS matrix_algorithm");
            statement.execute("CREATE TABLE matrix_algorithm (i INT, j INT, val INT, PRIMARY KEY (i, j))");
            StringBuilder insertQuery = new StringBuilder("INSERT INTO matrix_algorithm VALUES ");
            int[][] result = new int[matrixA.length][matrixB[0].length];
            //System.out.println("--- Matrix Calculator ---");
            for (int i = 0; i < matrixA.length; i++) {
                for (int j = 0; j < matrixB[0].length; j++) {
                    for (int k = 0; k < matrixA[0].length; k++) {
                        result[i][j] += matrixA[i][k] * matrixB[k][j];
                    }
                    insertQuery.append("(").append(i + 1).append(",").append(j + 1).append(",").append(result[i][j]).append("),");
                    //System.out.print(result[i][j] + " ");
                }
                //System.out.println();
            }
            insertQuery.deleteCharAt(insertQuery.length() - 1);
            statement.executeUpdate(insertQuery.toString());
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void ansatz1() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("CREATE VIEW C AS " +
                    "SELECT a.i, b.j, SUM(A.val * B.val) " +
                    "FROM a,  b " +
                    "WHERE a.j = b.i " +
                    "GROUP BY a.i, b.j");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createVectorTable(int[][][] matrix) {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS new_A, new_B");
            statement.execute("CREATE TABLE new_A (i INT, row INT[], PRIMARY KEY (i))");
            statement.execute("CREATE TABLE new_B (j INT, col INT[], PRIMARY KEY (j))");

            insertVector("new_A", matrix[0]);
            insertVector("new_B", matrix[1]);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertVector(String table_name, int[][] matrix){
        try (Statement statement = this.connection.createStatement()) {
            for (int i = 0; i < matrix.length; i++) {
                StringBuilder updateQueryA = new StringBuilder("INSERT INTO " + table_name + " VALUES (");
                updateQueryA.append(i + 1).append(", ARRAY[");
                for (int j = 0; j < matrix[0].length; j++) {
                    updateQueryA.append(matrix[i][j]);
                    if (j < matrix[i].length - 1) {
                        updateQueryA.append(",");
                    }
                }
                updateQueryA.append("])");
                statement.executeUpdate(updateQueryA.toString());
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void ansatz2() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS ansatz2");
            statement.execute("CREATE TABLE ansatz2 (i INT, j INT, val INT, PRIMARY KEY (i, j))");

            createFunction();

            statement.execute("INSERT INTO ansatz2 " +
                    "SELECT new_A.i, new_B.j, dotproduct(new_A.row, new_B.col) " +
                    "FROM new_A, new_B " +
                    "WHERE new_A.i = new_B.j " +
                    "GROUP BY new_a.i, new_b.j");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void createFunction() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                "CREATE OR REPLACE FUNCTION dotproduct(int[], int[]) RETURNS int AS $$\n" +
                        "DECLARE\n" +
                        "    result int := 0;\n" +
                        "    i int;\n" +
                        "    len int;\n" +
                        "BEGIN\n" +
                        "    len := LEAST(array_length($1, 1), array_length($2, 1));\n" +
                        "    FOR i IN 1 .. len LOOP\n" +
                        "            result := result + ($1[i] * $2[i]);\n" +
                        "        END LOOP;\n" +
                        "RETURN result;\n" +
                        "END;\n" +
                        "$$ LANGUAGE plpgsql;\n");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
