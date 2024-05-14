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
        e_matrix_mult.ansatz2(matrix);

        e_matrix_mult.closeConnection();
    }
    
}
