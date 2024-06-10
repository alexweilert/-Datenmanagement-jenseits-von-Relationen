package db;

import org.xml.sax.helpers.XMLReaderFactory;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import java.io.*;
import java.sql.*;
import java.util.*;

public class Berechnung_XPath_Achsen {
    Connection connection;

    public Berechnung_XPath_Achsen() throws SQLException, ClassNotFoundException {
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

    public void preprocessXMLFile(String inputFilePath, String outputFilePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
        FileWriter writer = new FileWriter(outputFilePath);
        String line;

        while ((line = reader.readLine()) != null) {
            line = line.replace("&uuml;", "ü");
            line = line.replace("&auml;", "ä");
            line = line.replace("&ouml;", "ö");
            // Add more replacements if needed
            writer.write(line + "\n");
        }
        reader.close();
        writer.close();
    }

    public List<Publication> parseXMLFile(String filePath) throws Exception {
        List<Publication> publications = new ArrayList<>();
        XMLReader xmlReader = XMLReaderFactory.createXMLReader();
        SAXHandler handler = new SAXHandler(publications);
        xmlReader.setContentHandler(handler);
        xmlReader.parse(filePath);
        return publications;
    }

    public Map<String, Map<String, List<Publication>>> transformData(List<Publication> publications) {
        Map<String, Map<String, List<Publication>>> transformedData = new HashMap<>();
        for (Publication pub : publications) {
            String venue = pub.journal != null ? pub.journal : pub.booktitle;
            String year = pub.year;
            transformedData.putIfAbsent(venue, new HashMap<>());
            transformedData.get(venue).putIfAbsent(year, new ArrayList<>());
            transformedData.get(venue).get(year).add(pub);
        }
        return transformedData;
    }

    public void createDatabaseSchema() throws Exception {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS publications");
            statement.execute("DROP TABLE IF EXISTS years");
            statement.execute("DROP TABLE IF EXISTS venues");

            statement.execute("CREATE TABLE venues (" +
                    "id SERIAL PRIMARY KEY, " +
                    "name TEXT NOT NULL)");

            statement.execute("CREATE TABLE years (" +
                    "id SERIAL PRIMARY KEY, " +
                    "year INTEGER NOT NULL, " +
                    "venue_id INTEGER, " +
                    "FOREIGN KEY (venue_id) REFERENCES venues (id))");

            statement.execute("CREATE TABLE publications (" +
                    "id SERIAL PRIMARY KEY, " +
                    "type TEXT, title TEXT, author TEXT, pages TEXT, volume TEXT, journal TEXT, booktitle TEXT, number TEXT, ee TEXT, url TEXT, " +
                    "year_id INTEGER, " +
                    "FOREIGN KEY (year_id) REFERENCES years (id))");
        }
    }

    public void insertData(Map<String, Map<String, List<Publication>>> transformedData) throws Exception {
        String venueSql = "INSERT INTO venues (name) VALUES (?)";
        String yearSql = "INSERT INTO years (year, venue_id) VALUES (?, ?)";
        String pubSql = "INSERT INTO publications (type, title, author, pages, volume, journal, booktitle, number, ee, url, year_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (
                PreparedStatement venueStmt = this.connection.prepareStatement(venueSql, Statement.RETURN_GENERATED_KEYS);
                PreparedStatement yearStmt = this.connection.prepareStatement(yearSql, Statement.RETURN_GENERATED_KEYS);
                PreparedStatement pubStmt = this.connection.prepareStatement(pubSql)
        ) {
            for (String venue : transformedData.keySet()) {
                venueStmt.setString(1, venue);
                venueStmt.executeUpdate();
                ResultSet venueRs = venueStmt.getGeneratedKeys();
                if (venueRs.next()) {
                    int venueId = venueRs.getInt(1);

                    for (String year : transformedData.get(venue).keySet()) {
                        yearStmt.setInt(1, Integer.parseInt(year));
                        yearStmt.setInt(2, venueId);
                        yearStmt.executeUpdate();
                        ResultSet yearRs = yearStmt.getGeneratedKeys();
                        if (yearRs.next()) {
                            int yearId = yearRs.getInt(1);

                            for (Publication pub : transformedData.get(venue).get(year)) {
                                pubStmt.setString(1, pub.type);
                                pubStmt.setString(2, pub.title);
                                pubStmt.setString(3, pub.author);
                                pubStmt.setString(4, pub.pages);
                                pubStmt.setString(5, pub.volume);
                                pubStmt.setString(6, pub.journal);
                                pubStmt.setString(7, pub.booktitle);
                                pubStmt.setString(8, pub.number);
                                pubStmt.setString(9, pub.ee);
                                pubStmt.setString(10, pub.url);
                                pubStmt.setInt(11, yearId);
                                pubStmt.executeUpdate();
                            }
                        }
                    }
                }
            }
        }
    }

    public void verifyImport() throws Exception {
        try (Statement statement = this.connection.createStatement()) {
            ResultSet rs = statement.executeQuery(
                    "SELECT venues.name, years.year, publications.title, publications.author " +
                            "FROM publications " +
                            "JOIN years ON publications.year_id = years.id " +
                            "JOIN venues ON years.venue_id = venues.id " +
                            "ORDER BY venues.name, years.year"
            );

            while (rs.next()) {
                System.out.println(rs.getString("name") + " | " +
                        rs.getInt("year") + " | " +
                        rs.getString("title") + " | " +
                        rs.getString("author"));
            }

            rs.close();
        }
    }
}