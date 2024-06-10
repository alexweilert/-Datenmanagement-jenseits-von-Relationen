package db;

import java.sql.SQLException;

public class Effiziente_Matrixmultiplikation_Main {
    
    public static void effiziente_matrixmultiplikation(String[] args) throws SQLException, ClassNotFoundException {
        Effiziente_Matrixmultiplikation e_matrix_mult = new Effiziente_Matrixmultiplikation();
        int[][][] matrix;
        e_matrix_mult.openConnection();

        matrix = e_matrix_mult.generate(5, 0.5);
        e_matrix_mult.ansatz0(matrix);
        e_matrix_mult.ansatz1();
        e_matrix_mult.createArrayTable(matrix);
        e_matrix_mult.createFunction();
        e_matrix_mult.ansatz2();
        benchmark(e_matrix_mult);
        System.out.println("Connection closed");

        e_matrix_mult.closeConnection();
    }


    public static void benchmark(Effiziente_Matrixmultiplikation e_matrix_mult) {
        int[][][] matrix;
        System.out.println("Test 1: Extending size of Matrix:");
        for(int i = 3; i <= 10; i++) {
            long startTime = System.currentTimeMillis();
            System.out.println("L: " + i + ", Sparsity: 0.5");
            matrix = e_matrix_mult.generate((int) Math.pow(2, i), 0.5);
            System.out.println("Matrix in " + (System.currentTimeMillis() - startTime) + " ms");
            e_matrix_mult.createArrayTable(matrix);
            System.out.println("Array in " + (System.currentTimeMillis() - startTime) + " ms");
            benchmark_calculation(e_matrix_mult, matrix);
        }
        System.out.println("Test 2: Extending Sparsity of Matrix:");
        for(double i = 0.1; i < 0.99; i += 0.1) {
            long startTime = System.currentTimeMillis();
            System.out.println("----- ----- -----");
            System.out.println("L: 16, Sparsity: " + i);
            matrix = e_matrix_mult.generate((int) Math.pow(2, 4), i);
            System.out.println("Matrix in " + (System.currentTimeMillis() - startTime) + " ms");
            e_matrix_mult.createArrayTable(matrix);
            System.out.println("Array in " + (System.currentTimeMillis() - startTime) + " ms");
            benchmark_calculation(e_matrix_mult, matrix);
        }
    }

    public static void benchmark_calculation(Effiziente_Matrixmultiplikation e_matrix_mult, int[][][] matrix) {
        long startTime = System.currentTimeMillis();
        int counter = 0;
        while (startTime >= System.currentTimeMillis() - 60000) {
            e_matrix_mult.ansatz0(matrix);
            counter++;
        }
        System.out.println("A0 in " + (System.currentTimeMillis() - startTime) + " ms " + counter + " iterations");
        startTime = System.currentTimeMillis();
        int counter1 = 0;
        while (startTime >= System.currentTimeMillis() - 60000) {
            e_matrix_mult.ansatz1();
            counter1++;
        }
        System.out.println("A1 in " + (System.currentTimeMillis() - startTime) + " ms " + counter1 + " iterations");
        startTime = System.currentTimeMillis();
        int counter2 = 0;
        while (startTime >= System.currentTimeMillis() - 60000) {
            e_matrix_mult.ansatz2();
            counter2++;
        }
        System.out.println("A2 in " + (System.currentTimeMillis() - startTime) + " ms " + counter2 + " iterations");
    }

}
