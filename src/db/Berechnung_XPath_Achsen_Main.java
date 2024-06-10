package db;

import java.util.List;
import java.util.Map;

import static db.Berechnung_XPath_Achsen.*;

public class Berechnung_XPath_Achsen_Main {

    public static void berechnung_xpath_achsen_main(String[] args) {
        try {
            Berechnung_XPath_Achsen bxpam = new Berechnung_XPath_Achsen();
            bxpam.openConnection();

            bxpam.preprocessXMLFile("src/db/toy_example.txt", "src/db/toy_example_processed.txt");

            List<Publication> publications = bxpam.parseXMLFile("src/db/toy_example_processed.txt");

            Map<String, Map<String, List<Publication>>> transformedData = bxpam.transformData(publications);

            bxpam.createTables();

            bxpam.insertData(transformedData);

            bxpam.verifyImport();

            bxpam.closeConnection();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
