package db;

import java.sql.SQLException;

import static db.Berechnung_XPath_Achsen_Main.berechnung_xpath_achsen_main;
import static db.Effiziente_Matrixmultiplikation_Main.*;
import static db.Sparsity_Main.*;

public class Main {

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        // Auskommentieren, um Projekt 1 auszuführen
        // sparsity_in_ecommerce(args);

        // Auskommentieren, um Projekt 2 auszuführen
        // effiziente_matrixmultiplikation(args);

        // Auskommentieren, um Projekt 3 auszuführen
        berechnung_xpath_achsen_main(args);
    }
}