package db;

import org.xml.sax.helpers.XMLReaderFactory;
import org.xml.sax.XMLReader;

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
            writer.write(line + "\n");
        }

        reader.close();
        writer.close();
    }

    public List<Publication> parseXMLFile(String filePath) throws Exception {
        List<Publication> publications = new ArrayList<>();
        List<Node> nodes = new ArrayList<>();
        List<Edge> edges = new ArrayList<>();
        XMLReader xmlReader = XMLReaderFactory.createXMLReader();
        SAXHandler handler = new SAXHandler(publications, nodes, edges);
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

    public void createTables() throws Exception {
        try (Statement statement = this.connection.createStatement()) {

            statement.execute("DROP TABLE IF EXISTS publications, years, venues, node, edge");

            statement.execute("CREATE TABLE venues (id SERIAL PRIMARY KEY, name TEXT)");

            statement.execute("CREATE TABLE years (id SERIAL PRIMARY KEY, year INTEGER, venue_id INTEGER, " +
                    "FOREIGN KEY (venue_id) REFERENCES venues (id))");

            statement.execute("CREATE TABLE publications (id SERIAL PRIMARY KEY,type TEXT, title TEXT, author TEXT, " +
                    "pages TEXT, volume TEXT, journal TEXT, booktitle TEXT, number TEXT, ee TEXT, url TEXT, year_id INTEGER, " +
                    "FOREIGN KEY (year_id) REFERENCES years (id))");

            statement.execute("CREATE TABLE node (id SERIAL PRIMARY KEY,s_id TEXT, type TEXT, content TEXT)");

            statement.execute("CREATE TABLE edge (parents INT, childs INT)");
        }
    }

    public void insertData(Map<String, Map<String, List<Publication>>> transformedData) throws Exception {
        String venueSql = "INSERT INTO venues (name) VALUES (?)";
        String yearSql = "INSERT INTO years (year, venue_id) VALUES (?, ?)";
        String pubSql = "INSERT INTO publications (type, title, author, pages, volume, journal, booktitle, number, ee, url, year_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        String nodeSql = "INSERT INTO node (s_id, type, content) VALUES (?, ?, ?)";
        String edgeSql = "INSERT INTO edge (parents, childs) VALUES (?, ?)";
        int from = 0;
        int to = 1;
        try (
            PreparedStatement venueStmt = this.connection.prepareStatement(venueSql, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement yearStmt = this.connection.prepareStatement(yearSql, Statement.RETURN_GENERATED_KEYS);
            PreparedStatement pubStmt = this.connection.prepareStatement(pubSql);
            PreparedStatement nodeStmt = this.connection.prepareStatement(nodeSql);
            PreparedStatement edgeStmt = this.connection.prepareStatement(edgeSql);
        ) {
            nodeStmt.setString(1, "bib");
            nodeStmt.setString(2, "bib");
            nodeStmt.setString(3, null);
            nodeStmt.executeUpdate();
            edgeStmt.setInt(1, from);
            from++;
            edgeStmt.setInt(2, to);
            to++;
            edgeStmt.executeUpdate();

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
                                nodeStmt.setString(1, pub.venue);
                                nodeStmt.setString(2, "venue");
                                nodeStmt.setString(3, null);
                                nodeStmt.executeUpdate();
                                edgeStmt.setInt(1, 1);
                                edgeStmt.setInt(2, to);
                                from = to;
                                to++;
                                edgeStmt.executeUpdate();

                                nodeStmt.setString(1, pub.venue_year);
                                nodeStmt.setString(2, "year");
                                nodeStmt.setString(3, null);
                                nodeStmt.executeUpdate();
                                edgeStmt.setInt(1, from);
                                edgeStmt.setInt(2, to);
                                from++;
                                to++;
                                edgeStmt.executeUpdate();

                                pubStmt.setString(1, pub.type);

                                nodeStmt.setString(1, pub.article);
                                nodeStmt.setString(2, pub.type);
                                nodeStmt.setString(3, null);
                                nodeStmt.executeUpdate();
                                nodeStmt.executeUpdate();
                                edgeStmt.setInt(1, from);
                                edgeStmt.setInt(2, to);
                                to++;
                                edgeStmt.executeUpdate();


                                pubStmt.setString(3, pub.author);
                                String[] authors = pub.author.split(",");
                                for(int i = 0; i < authors.length; i++) {
                                    nodeStmt.setString(1, null);
                                    nodeStmt.setString(2, "author");
                                    nodeStmt.setString(3, authors[i].trim());
                                    nodeStmt.executeUpdate();
                                    edgeStmt.setInt(1, from);
                                    edgeStmt.setInt(2, to);
                                    to++;
                                    edgeStmt.executeUpdate();
                                }

                                pubStmt.setString(2, pub.title);
                                if(pub.title != null) {
                                    nodeStmt.setString(1, null);
                                    nodeStmt.setString(2, "title");
                                    nodeStmt.setString(3, pub.title);
                                    nodeStmt.executeUpdate();
                                    edgeStmt.setInt(1, from);
                                    edgeStmt.setInt(2, to);
                                    to++;
                                    edgeStmt.executeUpdate();
                                }

                                pubStmt.setString(4, pub.pages);
                                if(pub.pages != null){
                                    nodeStmt.setString(1, null);
                                    nodeStmt.setString(2, "pages");
                                    nodeStmt.setString(3, pub.pages);
                                    nodeStmt.executeUpdate();
                                    edgeStmt.setInt(1, from);
                                    edgeStmt.setInt(2, to);
                                    to++;
                                    edgeStmt.executeUpdate();
                                }

                                if(pub.year != null) {
                                    nodeStmt.setString(1, null);
                                    nodeStmt.setString(2, "year");
                                    nodeStmt.setString(3, pub.year);
                                    nodeStmt.executeUpdate();
                                    edgeStmt.setInt(1, from);
                                    edgeStmt.setInt(2, to);
                                    to++;
                                    edgeStmt.executeUpdate();
                                }


                                pubStmt.setString(5, pub.volume);
                                if(pub.volume != null) {
                                    nodeStmt.setString(1, null);
                                    nodeStmt.setString(2, "volume");
                                    nodeStmt.setString(3, pub.volume);
                                    nodeStmt.executeUpdate();
                                    edgeStmt.setInt(1, from);
                                    edgeStmt.setInt(2, to);
                                    to++;
                                    edgeStmt.executeUpdate();
                                }
                                pubStmt.setString(6, pub.journal);
                                if(pub.journal != null) {
                                    nodeStmt.setString(1, null);
                                    nodeStmt.setString(2, "journal");
                                    nodeStmt.setString(3, pub.journal);
                                    nodeStmt.executeUpdate();
                                    edgeStmt.setInt(1, from);
                                    edgeStmt.setInt(2, to);
                                    to++;
                                    edgeStmt.executeUpdate();
                                }

                                pubStmt.setString(7, pub.booktitle);
                                if(pub.booktitle != null) {
                                    nodeStmt.setString(1, null);
                                    nodeStmt.setString(2, "booktitle");
                                    nodeStmt.setString(3, pub.booktitle);
                                    nodeStmt.executeUpdate();
                                    edgeStmt.setInt(1, from);
                                    edgeStmt.setInt(2, to);
                                    to++;
                                    edgeStmt.executeUpdate();
                                }

                                pubStmt.setString(8, pub.number);
                                if(pub.number != null) {
                                    nodeStmt.setString(1, null);
                                    nodeStmt.setString(2, "number");
                                    nodeStmt.setString(3, pub.number);
                                    nodeStmt.executeUpdate();
                                    edgeStmt.setInt(1, from);
                                    edgeStmt.setInt(2, to);
                                    to++;
                                    edgeStmt.executeUpdate();
                                }
                                pubStmt.setString(9, pub.ee);
                                if(pub.ee != null) {
                                    nodeStmt.setString(1, null);
                                    nodeStmt.setString(2, "ee");
                                    nodeStmt.setString(3, pub.ee);
                                    nodeStmt.executeUpdate();
                                    edgeStmt.setInt(1, from);
                                    edgeStmt.setInt(2, to);
                                    to++;
                                    edgeStmt.executeUpdate();
                                }
                                pubStmt.setString(10, pub.url);
                                if(pub.url != null) {
                                    nodeStmt.setString(1, null);
                                    nodeStmt.setString(2, "url");
                                    nodeStmt.setString(3, pub.url);
                                    nodeStmt.executeUpdate();
                                    edgeStmt.setInt(1, from);
                                    edgeStmt.setInt(2, to);
                                    to++;
                                    edgeStmt.executeUpdate();
                                }

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