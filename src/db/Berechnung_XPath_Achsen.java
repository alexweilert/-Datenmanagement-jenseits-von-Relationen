package db;

import org.xml.sax.helpers.XMLReaderFactory;
import org.xml.sax.XMLReader;

import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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
        int icde = 0, vldb = 0, sigmod = 0;

        String publication = "";

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
                    if(isArticel(line) || isBeginofArticle(line)) {
                        publication = line;
                    }
                    if(line.contains("><") && isInsidePublication) {

                        String splitter = "<" + line.split("><")[1];
                        writer.write(splitter + "\n");
                        isInsidePublication = false;

                    } else
                        writer.write(line + "\n");

                    while ((line = reader.readLine()) != null) {
                        line = replace(line);
                        if(isArticel(line) || isBeginofArticle(line)) {
                            publication = line;
                        }
                        if( ( line.contains("editor") || line.contains("author")) && ( line.contains("Nikolaus Augsten") || line.contains("Augsten Nikolaus") )) {
                            if(publication.contains("conf/icde/")){
                                icde++;
                            }
                            if(publication.contains("conf/vldb/") || publication.contains("journals/pvldb/")){
                                vldb++;
                            }
                            if(publication.contains("conf/sigmod") || publication.contains("journals/pacmmod/")){
                                sigmod++;
                            }
                        }
                        if( isArticel(line) || isEndOFArticle(line)) {

                            isInsidePublication = true;
                            initial = true;
                            break;

                        } else writer.write(line + "\n");
                    }

                } else line = reader.readLine();
            } else line = reader.readLine();

        }
        System.out.println("ICDE: " + icde + ", VLDB: " + vldb + ", SIGMOD: " + sigmod);
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
        return  line.contains("</proceedings><proceedings")    || line.contains("</inproceedings><proceedings")   || line.contains("</article><proceedings") ||
                line.contains("</proceedings><inproceedings")  || line.contains("</inproceedings><inproceedings") || line.contains("</article><inproceedings") ||
                line.contains("</proceedings><article")         || line.contains("</inproceedings><article")       || line.contains("</article><article");
    }

    public boolean isEndOFArticle(String line) {
        return line.equals("</proceedings>") || line.equals("</inproceedings>") || line.equals("</article>");
    }

    public boolean isBeginofArticle(String line){
        return line.startsWith("<proceedings") || line.startsWith("<inproceedings")   || line.startsWith("<article");
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
                    nodeStmt.setString(3, "v_year");
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

                        if (pub.booktitle != null){
                            id = insertIntoEdge(pubparents, id);
                            insertIntoNode(id, null, "booktitle", pub.booktitle);
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

    public void insertIntoNode(int id, String s_id, String type, String content) throws SQLException {
        PreparedStatement nodeStmt = this.connection.prepareStatement("INSERT INTO node (id, s_id, type, content) VALUES (?, ?, ?, ?)");
        nodeStmt.setInt(1, id);
        nodeStmt.setString(2, s_id);
        nodeStmt.setString(3, type);
        nodeStmt.setString(4, content);
        nodeStmt.executeUpdate();
    }

    public int insertIntoEdge(int parent, int children) throws SQLException {
        PreparedStatement edgeStmt = this.connection.prepareStatement("INSERT INTO edge (parents, childs) VALUES (?, ?)");
        children++;

        edgeStmt.setInt(1, parent);
        edgeStmt.setInt(2, children);
        edgeStmt.executeUpdate();
        return children;
    }

    public void createTables(){
        try (Statement statement = this.connection.createStatement()) {

            statement.execute("DROP INDEX IF EXISTS idx_accel_parent, idx_accel_pre, idx_edge_parents, idx_edge_childs");
            statement.execute("DROP TABLE IF EXISTS node, edge, accel, content, attribute, height");

            statement.execute("CREATE TABLE IF NOT EXISTS node (id int ,s_id TEXT, type TEXT, content TEXT)");

            statement.execute("CREATE TABLE IF NOT EXISTS edge (parents INT, childs INT)");

            statement.execute("CREATE TABLE IF NOT EXISTS accel (id INT PRIMARY KEY, post INT," +
                                            " s_id TEXT, parent INT, type TEXT)");

            statement.execute("CREATE TABLE IF NOT EXISTS content (id INT PRIMARY KEY, text TEXT)");

            statement.execute("CREATE TABLE IF NOT EXISTS attribute (id INT PRIMARY KEY, text TEXT)");

            statement.execute("CREATE TABLE IF NOT EXISTS height (id INT PRIMARY KEY, height INT)");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void populateSchema() throws SQLException {
        populateAccelTable();
        populateContentTable();
        populateAttributeTable();
    }

    public void populateAccelTable() throws SQLException {
        try (Statement statement = this.connection.createStatement()) {
            statement.executeUpdate("INSERT INTO accel (id, post, s_id, parent, type) SELECT id, 0, s_id, 0, type FROM node;");
            statement.executeUpdate("UPDATE accel a SET parent = e.parents FROM edge e WHERE a.id = e.childs;");
            calculatePrePostOrderNumbers();
        }
    }


    public void calculatePrePostOrderNumbers() throws SQLException {
        String getNodeQuery = "SELECT id, parent FROM accel";
        Map<Integer, List<Integer>> tree = new HashMap<>();
        Set<Integer> allNodes = new HashSet<>();
        Integer root = null;

        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(getNodeQuery)) {

            while (rs.next()) {
                int id = rs.getInt("id");
                int parent = rs.getInt("parent");
                allNodes.add(id);
                if (parent == 0 && root == null) {
                    root = id;
                } else {
                    tree.computeIfAbsent(parent, k -> new ArrayList<>()).add(id);
                }
            }
        }

        Map<Integer, Integer> preOrderMap = new HashMap<>();
        Map<Integer, Integer> postOrderMap = new HashMap<>();
        AtomicInteger order = new AtomicInteger(1);

        calculatePrePostOrder(tree, root, order, preOrderMap, postOrderMap, allNodes);

        try (PreparedStatement pstmt = connection.prepareStatement("UPDATE accel SET id = ?, post = ? WHERE id = ?")) {
            for (Integer id : preOrderMap.keySet()) {
                pstmt.setInt(1, id);
                pstmt.setInt(2, postOrderMap.get(id));
                pstmt.setInt(3, id);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    private void calculatePrePostOrder(Map<Integer, List<Integer>> tree, int node, AtomicInteger order, Map<Integer, Integer> preOrderMap, Map<Integer, Integer> postOrderMap, Set<Integer> allNodes) {
        preOrderMap.put(node, order.get());
        if (tree.containsKey(node)) {
            for (int child : tree.get(node)) {
                calculatePrePostOrder(tree, child, order, preOrderMap, postOrderMap, allNodes);
            }
        }
        postOrderMap.put(node, order.getAndIncrement());
        allNodes.remove(node);
    }

    private void populateContentTable() throws SQLException {
        String insertContent =
                "INSERT INTO content (id, text) " +
                        "SELECT a.id, a.type " +
                        "FROM accel a ";

        try (PreparedStatement pstmt = this.connection.prepareStatement(insertContent)) {
            pstmt.executeUpdate();
        }
    }

    private void populateAttributeTable() throws SQLException {
        String insertAttribute =
                "INSERT INTO attribute (id, text) " +
                        "SELECT a.id, a.s_id " +
                        "FROM accel a " +
                        "WHERE a.s_id IS NOT NULL";

        try (PreparedStatement pstmt = this.connection.prepareStatement(insertAttribute)) {
            pstmt.executeUpdate();
        }
    }

    public void calculateHeights() throws SQLException {
        String getNodeQuery = "SELECT id, parent FROM accel";
        Map<Integer, List<Integer>> tree = new HashMap<>();
        Set<Integer> allNodes = new HashSet<>();
        Integer root = null;

        // Abrufen der Knoten und Strukturierung des Baumes
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery(getNodeQuery)) {

            while (rs.next()) {
                int id = rs.getInt("id");
                int parent = rs.getInt("parent");
                allNodes.add(id);
                if (parent == 0 && root == null) {
                    root = id;
                } else {
                    tree.computeIfAbsent(parent, k -> new ArrayList<>()).add(id);
                }
            }
        }

        if (root == null) {
            throw new IllegalStateException("Root node not found");
        }

        // Dictionary zur Speicherung der Höhen
        Map<Integer, Integer> heightMap = new HashMap<>();

        // Höhenberechnung für alle Knoten
        for (Integer nodeId : allNodes) {
            calculateHeight(nodeId, tree, heightMap);
        }

        // Werte in die Tabelle "height" einfügen
        String insertQuery = "INSERT INTO height (id, height) VALUES (?, ?)";
        try (PreparedStatement pstmt = connection.prepareStatement(insertQuery)) {
            for (Integer nodeId : heightMap.keySet()) {
                pstmt.setInt(1, nodeId);
                pstmt.setInt(2, heightMap.get(nodeId));
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    private int calculateHeight(Integer nodeId, Map<Integer, List<Integer>> tree, Map<Integer, Integer> heightMap) {
        if (heightMap.containsKey(nodeId)) {
            return heightMap.get(nodeId);
        }

        Integer parentId = getParentId(nodeId, tree);
        if (parentId == null) {
            heightMap.put(nodeId, 0);
        } else {
            int parentHeight = calculateHeight(parentId, tree, heightMap);
            heightMap.put(nodeId, parentHeight + 1);
        }
        return heightMap.get(nodeId);
    }

    private Integer getParentId(Integer nodeId, Map<Integer, List<Integer>> tree) {
        for (Map.Entry<Integer, List<Integer>> entry : tree.entrySet()) {
            if (entry.getValue().contains(nodeId)) {
                return entry.getKey();
            }
        }
        return null;
    }
}
