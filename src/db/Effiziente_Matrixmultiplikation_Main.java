package db;

import java.sql.SQLException;

public class Effiziente_Matrixmultiplikation_Main {
    
    public static void  effiziente_matrixmultiplikation(String[] args) throws SQLException, ClassNotFoundException {
        Effiziente_Matrixmultiplikation e_matrix_mult = new Effiziente_Matrixmultiplikation();

        e_matrix_mult.openConnection();

        e_matrix_mult.generate(5, 0.5);

        e_matrix_mult.closeConnection();
    }
    
}
