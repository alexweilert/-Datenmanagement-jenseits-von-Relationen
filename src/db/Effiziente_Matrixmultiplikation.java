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
            matrix[0] = generateMatrixA(l, sparsity);
            matrix[1] = generateMatrixB(l, sparsity);
            insertMatrix("A", matrix[0]);
            insertMatrix("B", matrix[1]);

            //ansatz0(matrixA, matrixB); // Matrix Calculator per Algorithm
            //ansatz1();// Matrix Calculator per Select
            return matrix;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static int[][][] getArrays(int[][] a, int[][] b) {
        int[][][] result = new int[2][][]; // Ein 2D-Array, das zwei 2D-Arrays enthält
        result[0] = a;
        result[1] = b;
        return result;
    }

    public int[][] generateMatrixA(int l, double sparsity) {
        Random random = new Random();
        int[][] matrixA = new int[l - 1][l];
        System.out.println("--- Matrix A ---");
        for (int i = 0; i < (l - 1); i++) {
            for (int j = 0; j < l; j++) {
                if (random.nextDouble() > sparsity) {
                    matrixA[i][j] = random.nextInt(1, 11); // Random value between 0 and 10
                } else {
                    matrixA[i][j] = 0;
                }
                System.out.print(matrixA[i][j] + " ");
            }
            System.out.println();
        }
        return matrixA;
    }

    public int[][] generateMatrixB(int l, double sparsity) {
        Random random = new Random();
        int[][] matrixB = new int[l][l - 1];
        System.out.println("--- Matrix B ---");
        for (int i = 0; i < l; i++) {
            for (int j = 0; j < (l - 1); j++) {
                if (random.nextDouble() > sparsity) {
                    matrixB[i][j] = random.nextInt(1, 11);// Random value between 0 and 10
                } else {
                    matrixB[i][j] = 0;
                }
                System.out.print(matrixB[i][j] + " ");
            }
            System.out.println();
        }
        return matrixB;
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
            System.out.println("--- Matrix Calculator ---");
            for (int i = 0; i < matrixA.length; i++) {
                for (int j = 0; j < matrixB[0].length; j++) {
                    for (int k = 0; k < matrixA[0].length; k++) {
                        result[i][j] += matrixA[i][k] * matrixB[k][j];
                    }
                    insertQuery.append("(").append(i + 1).append(",").append(j + 1).append(",").append(result[i][j]).append("),");
                    System.out.print(result[i][j] + " ");
                }
                System.out.println();
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

    public void ansatz2(int[][][] matrix) {
        try (Statement statement = this.connection.createStatement()) {
            int[][] matrixA = matrix[0];
            int[][] matrixB = matrix[1];
            statement.execute("DROP TABLE IF EXISTS new_A, new_B");
            statement.execute("CREATE TABLE new_A (i INT, row_array INT[], PRIMARY KEY (i))");
            statement.execute("CREATE TABLE new_B (j INT, col_array INT[], PRIMARY KEY (j))");

            statement.execute("UPDATE new_A SET row_array = ARRAY(SELECT val FROM A WHERE i = A.i)");
            statement.execute("UPDATE new_B SET col_array = ARRAY(SELECT val FROM B WHERE j = B.j)");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
