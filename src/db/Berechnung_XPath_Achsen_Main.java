package db;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static db.Berechnung_XPath_Achsen.*;

public class Berechnung_XPath_Achsen_Main {



    public static void berechnung_xpath_achsen_main(String[] args) {
        try {

            Berechnung_XPath_Achsen bxpam = new Berechnung_XPath_Achsen();
            if(args.length == 0) {
                toyBeispiel(bxpam);
            } else if(args.length < 2) {
                System.err.println("Verwende: <file> <venues>");
            } else
                bxpam.openConnection();

            bxpam.preprocessXMLFile(args[0], "src/db/my_small_bib.xml", args[1]);

            List<Publication> publications = bxpam.parseXMLFile("src/db/my_small_bib.xml");

            Map<String, Map<String, List<Publication>>> transformedData = bxpam.transformData(publications);

            bxpam.createTables();

            bxpam.insertData(transformedData);

            bxpam.createFunctionXPathInEdgeModel();

            bxpam.fillSchema();

            bxpam.closeConnection();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }






    public static void toyBeispiel(Berechnung_XPath_Achsen bxpam){
        // ToyBeispiel
        try {
            bxpam.openConnection();

            bxpam.preprocessXMLFile("src/db/toy_example.txt", "src/db/toy_example_processed.txt");


            List<Publication> publications = bxpam.parseXMLFile("src/db/toy_example_processed.txt");

            Map<String, Map<String, List<Publication>>> transformedData = bxpam.transformData(publications);

            bxpam.createTables();

            bxpam.insertData(transformedData);

            bxpam.createFunctionXPathInEdgeModel();

            bxpam.fillSchema();

            bxpam.closeConnection();

        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


}
