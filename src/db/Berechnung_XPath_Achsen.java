package db;

import org.xml.sax.helpers.XMLReaderFactory;
import org.xml.sax.XMLReader;

import java.io.*;
import java.sql.*;
import java.util.*;

public class Berechnung_XPath_Achsen {
    Connection connection;

    public Berechnung_XPath_Achsen(){
    }

    public void openConnection() throws SQLException, ClassNotFoundException {
        Class.forName("org.postgresql.Driver");
        this.connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "alex");
    }

    public void closeConnection() throws SQLException {
        this.connection.close();
    }

    public void preprocessXMLFile(String inputFilePath, String outputFilePath) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
        FileWriter writer = new FileWriter(outputFilePath);
        String line;

        while ((line = reader.readLine()) != null) {
            line = replace(line);
            writer.write(line + "\n");
        }

        reader.close();
        writer.close();
    }

    public void preprocessXMLFile(String inputFilePath, String outputFilePath, String venueList) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(inputFilePath));
        FileWriter writer = new FileWriter(outputFilePath);
        writer.write("<bib>\n");
        boolean isInsidePublication = true;
        String line = reader.readLine();
        boolean initial = false;
        while (line != null) {

            line = replace(line);
            if(initial && isEndOFArticle(line)) {

                writer.write(line + "\n");
                initial = false;

            } else if (line.contains("key=")) {

                if(line.contains("><") && initial) {

                    String splitter = line.split("><")[0] + ">";
                    writer.write( splitter + "\n");
                    initial = false;

                } if(isImportant(line, venueList)) {

                    if(line.contains("><") && isInsidePublication) {

                        String splitter = "<" + line.split("><")[1];
                        writer.write(splitter + "\n");
                        isInsidePublication = false;

                    } else writer.write(line + "\n");


                    while ((line = reader.readLine()) != null) {
                        line = replace(line);

                        if( isArticel(line) || isEndOFArticle(line)) {

                            isInsidePublication = true;
                            initial = true;
                            break;

                        } else writer.write(line + "\n");
                    }

                } else line = reader.readLine();
            } else line = reader.readLine();

        }
        writer.write("</bib>");
        reader.close();
        writer.close();
    }

    public String replace(String line) {
        line = line.replace("&Agrave;", "À");
        line = line.replace("&Aacute;", "Á");
        line = line.replace("&Acirc;", "Â");
        line = line.replace("&Atilde;", "Ã");
        line = line.replace("&Auml;", "Ä");
        line = line.replace("&Aring;", "Å");
        line = line.replace("&AElig;", "Æ");
        line = line.replace("&Ccedil;", "Ç");
        line = line.replace("&Egrave;", "È");
        line = line.replace("&Eacute;", "É");
        line = line.replace("&Ecirc;", "Ê");
        line = line.replace("&Euml;", "Ë");
        line = line.replace("&Igrave;", "Ì");
        line = line.replace("&Iacute;", "Í");
        line = line.replace("&Icirc;", "Î");
        line = line.replace("&Iuml;", "Ï");
        line = line.replace("&ETH;", "Ð");
        line = line.replace("&Ntilde;", "Ñ");
        line = line.replace("&Ograve;", "Ò");
        line = line.replace("&Oacute;", "Ó");
        line = line.replace("&Ocirc;", "Ô");
        line = line.replace("&Otilde;", "Õ");
        line = line.replace("&Ouml;", "Ö");
        line = line.replace("&Oslash;", "Ø");
        line = line.replace("&Ugrave;", "Ù");
        line = line.replace("&Uacute;", "Ú");
        line = line.replace("&Ucirc;", "Û");
        line = line.replace("&Uuml;", "Ü");
        line = line.replace("&Yacute;", "Ý");
        line = line.replace("&THORN;", "Þ");
        line = line.replace("&szlig;", "ß");
        line = line.replace("&agrave;", "à");
        line = line.replace("&aacute;", "á");
        line = line.replace("&acirc;", "â");
        line = line.replace("&atilde;", "ã");
        line = line.replace("&auml;", "ä");
        line = line.replace("&aring;", "å");
        line = line.replace("&aelig;", "æ");
        line = line.replace("&ccedil;", "ç");
        line = line.replace("&egrave;", "è");
        line = line.replace("&eacute;", "é");
        line = line.replace("&ecirc;", "ê");
        line = line.replace("&euml;", "ë");
        line = line.replace("&igrave;", "ì");
        line = line.replace("&iacute;", "í");
        line = line.replace("&icirc;", "î");
        line = line.replace("&iuml;", "ï");
        line = line.replace("&eth;", "ð");
        line = line.replace("&ntilde;", "ñ");
        line = line.replace("&ograve;", "ò");
        line = line.replace("&oacute;", "ó");
        line = line.replace("&ocirc;", "ô");
        line = line.replace("&otilde;", "õ");
        line = line.replace("&ouml;", "ö");
        line = line.replace("&oslash;", "ø");
        line = line.replace("&ugrave;", "ù");
        line = line.replace("&uacute;", "ú");
        line = line.replace("&ucirc;", "û");
        line = line.replace("&uuml;", "ü");
        line = line.replace("&yacute;", "ý");
        line = line.replace("&thorn;", "þ");
        line = line.replace("&yuml;", "ÿ");
        line = line.replace("&amp;", "and");
        line = line.replace("&lt;", "<");
        line = line.replace("&gt;", ">");
        line = line.replace("&reg;", "®");
        line = line.replace("&micro;", "µ");
        line = line.replace("&times;", "×");
        return line;
    }

    public boolean isArticel(String line) {
        return  line.contains("</proceedings><proceedings>")    || line.contains("</inproceedings><proceedings>")   || line.contains("</article><proceedings>") ||
                line.contains("</proceedings><inproceedings>")  || line.contains("</inproceedings><inproceedings>") || line.contains("</article><inproceedings>") ||
                line.contains("</proceedings><article")         || line.contains("</inproceedings><article>")       || line.contains("</article><article>");
    }

    public boolean isEndOFArticle(String line) {
        return line.equals("</proceedings>") || line.equals("</inproceedings>") || line.equals("</article>");
    }

    public boolean isImportant(String line, String venueList) {
        String[] venues = venueList.split(",");
        if (line.contains("key")) {
            for (int i = 0; i < venues.length; i++) {
                if (venues[i].toLowerCase().equals("sigmod")) {
                    if (line.contains("journals/pacmmod/")) {
                        return true;
                    }
                    if (line.contains("conf/sigmod/")) {
                        return true;
                    }
                } else if (venues[i].toLowerCase().contains("vldb")) {
                    if (line.contains("journals/pvldb/")) {
                        return true;
                    }
                    if (line.contains("conf/vldb/")) {
                        return true;
                    }
                } else if (venues[i].toLowerCase().contains("icde")) {
                    if (line.contains("conf/icde/")) {
                        return true;
                    }
                }
            }
        }
        return false;
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
            String venue = pub.venue != null ? pub.venue : pub.booktitle;
            String year = pub.year;
            transformedData.putIfAbsent(venue, new HashMap<>());
            transformedData.get(venue).putIfAbsent(year, new ArrayList<>());
            transformedData.get(venue).get(year).add(pub);
        }

        // Sort the outer map by keys (venue)
        Map<String, Map<String, List<Publication>>> sortedData = new TreeMap<>(transformedData);

        // Sort the inner maps by keys (year) in descending order and then sort the publication lists
        for (Map.Entry<String, Map<String, List<Publication>>> entry : sortedData.entrySet()) {
            Map<String, List<Publication>> innerMap = entry.getValue();
            Map<String, List<Publication>> sortedInnerMap = new TreeMap<>(Collections.reverseOrder());

            for (Map.Entry<String, List<Publication>> innerEntry : innerMap.entrySet()) {
                List<Publication> publicationList = innerEntry.getValue();
                publicationList.sort(Comparator.comparing(p -> p.article)); // sort by title or any other attribute
                sortedInnerMap.put(innerEntry.getKey(), publicationList);
            }

            entry.setValue(sortedInnerMap);
        }
        return sortedData;
    }

    public void createTables(){
        try (Statement statement = this.connection.createStatement()) {

            statement.execute("DROP TABLE IF EXISTS node, edge, accel, content, attribute");

            statement.execute("CREATE TABLE node (id int ,s_id TEXT, type TEXT, content TEXT)");

            statement.execute("CREATE TABLE edge (parents INT, childs INT)");

            statement.execute("CREATE TABLE accel (pre INT, post INT, parent INT, kind VARCHAR(255), name VARCHAR(255))");

            statement.execute("CREATE TABLE content (pre INT, text TEXT)");

            statement.execute("CREATE TABLE attribute (pre INT, text TEXT)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertData(Map<String, Map<String, List<Publication>>> data) throws Exception {
        try (Statement statement = this.connection.createStatement()) {
            int parent = 0;
            int children = 0;
            int id = 0;
            PreparedStatement nodeStmt = this.connection.prepareStatement("INSERT INTO node (id, s_id, type, content) VALUES (?, ?, ?, ?)");

            nodeStmt.setInt(1, 0);
            nodeStmt.setString(2, "bib");
            nodeStmt.setString(3, "bib");
            nodeStmt.setString(4, null);
            nodeStmt.executeUpdate();

            String isParent = data.keySet().stream().findFirst().get();
            for (String venue : data.keySet()) {
                if(isParent.equals(venue)) {
                    children = insertIntoEdge(id, children);
                    id++;
                } else {
                    children = insertIntoEdge(parent, id);
                    id++;
                }

                nodeStmt.setInt(1, id);
                nodeStmt.setString(2, venue);
                nodeStmt.setString(3, "venue");
                nodeStmt.setString(4, null);
                nodeStmt.executeUpdate();


                int yearchilds = children;
                for (String year : data.get(venue).keySet()) {
                    yearchilds = insertIntoEdge(children, id);
                    id++;

                    String venue_year = venue + "_" + year;
                    nodeStmt.setInt(1, id);
                    nodeStmt.setString(2, venue_year);
                    nodeStmt.setString(3, "year");
                    nodeStmt.setString(4, null);
                    nodeStmt.executeUpdate();


                    int pubparents;
                    for (Publication pub : data.get(venue).get(year)) {
                        id = insertIntoEdge(yearchilds, id);
                        insertIntoNode(id, pub.article, pub.type, null);

                        pubparents = id;

                        if (pub.author != null) {
                            String[] authors = pub.author.split(",");
                            for (int i = 0; i < authors.length; i++) {
                                id = insertIntoEdge(pubparents, id);
                                insertIntoNode(id, null, "author", authors[i].trim());
                            }
                        }

                        if (pub.title != null) {
                            id = insertIntoEdge(pubparents, id);
                            insertIntoNode(id, null, "title", pub.title);
                        }
                        if (pub.pages != null) {
                            id = insertIntoEdge(pubparents, id);
                            insertIntoNode(id, null, "pages", pub.pages);
                        }
                        if (pub.year != null) {
                            id = insertIntoEdge(pubparents, id);
                            insertIntoNode(id, null, "year", pub.year);
                        }
                        if (pub.volume != null) {
                            id = insertIntoEdge(pubparents, id);
                            insertIntoNode(id, null, "volume", pub.volume);
                        }
                        if (pub.journal != null) {
                            id = insertIntoEdge(pubparents, id);
                            insertIntoNode(id, null, "journal", pub.journal);
                        }
                        if (pub.booktitle != null) {
                            id = insertIntoEdge(pubparents, id);
                            insertIntoNode(id, null, "booktitle", pub.booktitle);
                        }

                        if (pub.number != null) {
                            id = insertIntoEdge(pubparents, id);
                            insertIntoNode(id, null, "number", pub.number);
                        }
                        if (pub.ee != null) {
                            String[] ees = pub.ee.split(",");
                            for (int i = 0; i < ees.length; i++) {
                                id = insertIntoEdge(pubparents, id);
                                insertIntoNode(id, null, "ee", ees[i].trim());
                            }
                        }
                        if (pub.crossref != null){
                            id = insertIntoEdge(pubparents, id);
                            insertIntoNode(id, null, "crossref", pub.crossref);
                        }

                        if (pub.url != null) {
                            id = insertIntoEdge(pubparents, id);
                            insertIntoNode(id, null, "url", pub.url);
                        }
                    }
                }
            }
        }
    }

    public void insertIntoNode(int id, String s_id, String type, String content){
        try (Statement statement = this.connection.createStatement()) {
            PreparedStatement nodeStmt = this.connection.prepareStatement("INSERT INTO node (id, s_id, type, content) VALUES (?, ?, ?, ?)");

            nodeStmt.setInt(1, id);
            nodeStmt.setString(2, s_id);
            nodeStmt.setString(3, type);
            nodeStmt.setString(4, content);
            nodeStmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public int insertIntoEdge(int parent, int children){
        try (Statement statement = this.connection.createStatement()) {
            PreparedStatement edgeStmt = this.connection.prepareStatement("INSERT INTO edge (parents, childs) VALUES (?, ?)");
            children++;

            edgeStmt.setInt(1, parent);
            edgeStmt.setInt(2, children);
            edgeStmt.executeUpdate();

        } catch (SQLException e) {
            throw new RuntimeException(e);
        } return children;
    }

    public void fillSchema() throws SQLException {
        calculatePrePostOrder();
        populateAccelTable();
        populateContentTable();
        populateAttributeTable();
    }

    // Methode zur Berechnung der Pre- und Post-Order-Werte mittels SQL
    private void calculatePrePostOrder() throws SQLException {
        // Temporäre Tabellen für die Berechnungen erstellen
        String createTempTables =
                "DROP TABLE IF EXISTS temp_nodes;" +
                        "CREATE TEMP TABLE temp_nodes AS " +
                        "WITH RECURSIVE tree AS (" +
                        "    SELECT id, 1 AS level, CAST(1 AS INT) AS pre " +
                        "    FROM node " +
                        "    WHERE id = 1 " +  // Assuming 1 is the root node
                        "    UNION ALL " +
                        "    SELECT n.id, t.level + 1, CAST(ROW_NUMBER() OVER(ORDER BY t.pre) + t.pre AS INT) " +
                        "    FROM node n " +
                        "    JOIN edge e ON n.id = e.childs " +
                        "    JOIN tree t ON e.parents = t.id " +
                        ") " +
                        "SELECT id, level, pre, CAST(ROW_NUMBER() OVER (ORDER BY level DESC, pre DESC) AS INT) AS post " +
                        "FROM tree;";


        try (Statement stmt = this.connection.createStatement()) {
            stmt.execute(createTempTables);
        }
    }

    // Daten aus den temporären Tabellen in die Zieltabelle einfügen
    private void populateAccelTable() throws SQLException {
        String insertAccel =
                "INSERT INTO accel (pre, post, parent, kind, name) " +
                        "SELECT tn.pre, tn.post, e.parents, n.type, n.s_id " +
                        "FROM temp_nodes tn " +
                        "JOIN edge e ON tn.id = e.childs " +
                        "JOIN node n ON e.childs = n.id;";

        try (PreparedStatement pstmt = this.connection.prepareStatement(insertAccel)) {
            pstmt.executeUpdate();
        }
    }

    // Daten in die Tabelle content einfügen
    private void populateContentTable() throws SQLException {
        String insertContent =
                "INSERT INTO content (pre, text) " +
                        "SELECT tn.pre, n.content " +
                        "FROM temp_nodes tn " +
                        "JOIN node n ON tn.id = n.id;";

        try (PreparedStatement pstmt = this.connection.prepareStatement(insertContent)) {
            pstmt.executeUpdate();
        }
    }

    // Daten in die Tabelle attribute einfügen
    private void populateAttributeTable() throws SQLException {
        String insertAttribute =
                "INSERT INTO attribute (pre, text) " +
                        "SELECT tn.pre, n.s_id " +
                        "FROM temp_nodes tn " +
                        "JOIN node n ON tn.id = n.id;";

        try (PreparedStatement pstmt = this.connection.prepareStatement(insertAttribute)) {
            pstmt.executeUpdate();
        }
    }


    public void createFunctionXPathInEdgeModel(){
        createAncestors();
        createDescendants();
        createFollowingSiblings();
        createPreceedingSiblings();
    }

    private void createAncestors(){
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("CREATE OR REPLACE FUNCTION get_ancestors(v INT) " +
                    "RETURNS TABLE(ancestor INT) AS $$ " +
                    "BEGIN RETURN QUERY " +
                    "WITH RECURSIVE Ancestors AS ( " +
                    "SELECT e.parents FROM edge e WHERE e.childs = v " +
                    "UNION " +
                    "SELECT e.parents FROM edge e " +
                    "INNER JOIN Ancestors a ON e.childs = a.parents ) " +
                    "SELECT parents FROM Ancestors; " +
                    "END; $$ LANGUAGE plpgsql;");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createDescendants(){
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("CREATE OR REPLACE FUNCTION get_descendants(v INT) " +
                    "RETURNS TABLE(descendant INT) AS $$ " +
                    "BEGIN RETURN QUERY " +
                    "WITH RECURSIVE Descendants AS ( " +
                    "SELECT e.childs FROM edge e WHERE e.parents = v " +
                    "UNION " +
                    "SELECT e.childs FROM edge e " +
                    "INNER JOIN Descendants d ON e.parents = d.childs ) " +
                    "SELECT childs FROM Descendants; " +
                    "END; $$ LANGUAGE plpgsql;");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createFollowingSiblings(){
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("CREATE OR REPLACE FUNCTION get_following_siblings(v INT) " +
                    "RETURNS TABLE(following_siblings INT) AS $$ " +
                    "BEGIN RETURN QUERY " +
                    "SELECT n2.id FROM node n1 " +
                    "JOIN edge e1 ON n1.id = e1.childs " +
                    "JOIN edge e2 ON e1.parents = e2.parents " +
                    "JOIN node n2 ON e2.childs = n2.id " +
                    "WHERE n1.id = v AND e2.childs != v AND n2.id > n1.id; " +
                    "END; $$ LANGUAGE plpgsql;");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createPreceedingSiblings (){
        try (Statement statement = this.connection.createStatement()) {
            statement.execute("CREATE OR REPLACE FUNCTION get_preceeding_siblings(v INT) " +
                    "RETURNS TABLE(prec_sibl INT) AS $$ " +
                    "BEGIN RETURN QUERY " +
                    "SELECT n2.id FROM node n1 " +
                    "JOIN edge e1 ON n1.id = e1.childs " +
                    "JOIN edge e2 ON e1.parents = e2.parents " +
                    "JOIN node n2 ON e2.childs = n2.id " +
                    "WHERE n1.id = v AND e2.childs != v AND n2.id < n1.id; " +
                    "END; $$ LANGUAGE plpgsql;");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
