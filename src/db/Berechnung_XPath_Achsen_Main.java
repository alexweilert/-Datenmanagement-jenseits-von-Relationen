package db;

import java.util.List;
import java.util.Map;

import static db.Berechnung_XPath_Achsen.*;

public class Berechnung_XPath_Achsen_Main {

    public static void berechnung_xpath_achsen_main(String[] args) {
        try {
            Berechnung_XPath_Achsen bxpam = new Berechnung_XPath_Achsen();
            bxpam.openConnection();
            // Preprocess the XML File
            bxpam.preprocessXMLFile("src/db/toy_example.txt", "src/db/toy_example_processed.txt");

            // Step 1: Parse the XML File
            List<Publication> publications = bxpam.parseXMLFile("src/db/toy_example_processed.txt");
            // Step 2: Transform the Data
            Map<String, Map<String, List<Publication>>> transformedData = bxpam.transformData(publications);

            // Step 3: Create the Database Schema
            bxpam.createDatabaseSchema();

            // Step 4: Insert the Data
            bxpam.insertData(transformedData);

            // Step 5: Verify the Import
            bxpam.verifyImport();

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
