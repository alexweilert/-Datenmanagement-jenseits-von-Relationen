package db;

import java.sql.SQLException;

import static db.Effiziente_Matrixmultiplikation_Main.*;
import static db.Sparsity_Main.*;

public class Main {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // Auskommentieren, wenn Projekt 1 ausgeführt werden soll
        // sparsity_in_ecommerce(args);

        // Auskommentieren, wenn Projekt 2 ausgeführt werden soll
        effiziente_matrixmultiplikation(args);
    }
}