package db;

import java.sql.SQLException;

public class Effiziente_Matrixmultiplikation_Main {
    
    public static void  effiziente_matrixmultiplikation(String[] args) throws SQLException, ClassNotFoundException {
        Effiziente_Matrixmultiplikation e_matrix_mult = new Effiziente_Matrixmultiplikation();
        int[][][] matrix;
        e_matrix_mult.openConnection();

        matrix = e_matrix_mult.generate(5, 0.5);
        e_matrix_mult.ansatz0(matrix);
        e_matrix_mult.ansatz1();
        e_matrix_mult.createVectorTable(matrix);
        e_matrix_mult.ansatz2();

        //benchmark(e_matrix_mult);

        e_matrix_mult.closeConnection();
    }

    public static void benchmark(Effiziente_Matrixmultiplikation e_matrix_mult) throws SQLException, ClassNotFoundException {
        int[][][] matrix;
        System.out.println("Benchmark enrolling");
        long startTime = System.currentTimeMillis();
        System.out.println("Test 1: Extending size of Matrix:");
        for(int i = 3; i < 13; i++) {
            System.out.println("Testing with: L: " + i + ", Sparsity: 0.5");
            matrix = e_matrix_mult.generate((int) Math.pow(2, i), 0.5);
            System.out.println("Matrix generated in " + (System.currentTimeMillis() - startTime) + " ms");
            benchmark_calculation(e_matrix_mult, matrix, startTime);
        }

        System.out.println("Test 2: Extending Sparsity of Matrix:");
        for(double i = 0.1; i < 0.99; i += 0.1) {
            System.out.println("----- ----- -----");
            System.out.println("L: 10, Sparsity: " + i);
            matrix = e_matrix_mult.generate((int) Math.pow(2, 3), i);
            System.out.println("Matrix in " + (System.currentTimeMillis() - startTime) + " ms");
            benchmark_calculation(e_matrix_mult, matrix, startTime);
        }
    }

    public static void benchmark_calculation(Effiziente_Matrixmultiplikation e_matrix_mult, int[][][] matrix, long startTime) {
        e_matrix_mult.ansatz0(matrix);
        System.out.println("A0 Finished in " + (System.currentTimeMillis() - startTime) + " ms");
        e_matrix_mult.ansatz1();
        System.out.println("A1 Finished in " + (System.currentTimeMillis() - startTime) + " ms");
        e_matrix_mult.createVectorTable(matrix);
        System.out.println("A2 Finished in " + (System.currentTimeMillis() - startTime) + " ms");
    }
}
