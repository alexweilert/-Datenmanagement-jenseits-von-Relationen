package db;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static db.Berechnung_XPath_Achsen.*;

public class Berechnung_XPath_Achsen_Main {



    public static void berechnung_xpath_achsen_main(String[] args) {
        try {
            Berechnung_XPath_Achsen bxpam = new Berechnung_XPath_Achsen();
            if (args.length == 0) {
                toyBeispiel(bxpam);
            } else if (args.length == 1) {
                benchmark(args, bxpam);
            } else if (args.length < 2) {
                System.err.println("Verwende: <file> <venues>");
            } else {

                bxpam.openConnection();

                bxpam.preprocessXMLFile(args[0], "dblp/dblp.xml", args[1]);

                List<Publication> publications = bxpam.parseXMLFile("src/db/my_small_bib.xml");

                Map<String, Map<String, List<Publication>>> transformedData = bxpam.transformData(publications);

                bxpam.createTables();

                bxpam.insertData(transformedData);

                bxpam.populateSchema();

                bxpam.calculateHeights();

                EdgeModelFunctions emf = new EdgeModelFunctions(bxpam.connection);
                emf.createFunctionXPathInEdgeModel();

                XPathFunctions xpath = new XPathFunctions(bxpam.connection);
                xpath.createFunctionXPath();

                XPathSmallerWindow xPathSmallerWindow = new XPathSmallerWindow(bxpam.connection);
                xPathSmallerWindow.createFunctionXPathSmallWindow();

                bxpam.closeConnection();
            }
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

            bxpam.populateSchema();

            EdgeModelFunctions emf = new EdgeModelFunctions(bxpam.connection);
            emf.createFunctionXPathInEdgeModel();

            XPathFunctions xpath = new XPathFunctions(bxpam.connection);
            xpath.createFunctionXPath();

            bxpam.closeConnection();

        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void benchmark(String[] args, Berechnung_XPath_Achsen bxpam) throws Exception {
        bxpam.openConnection();
        System.out.println("Preparing Benchmark.");
        //bxpam.preprocessXMLFile(args[0], "dblp/dblp.xml", "SIGMOD,VLDB,ICDE");
        //List<Publication> publications = bxpam.parseXMLFile("src/db/my_small_bib.xml");
        //Map<String, Map<String, List<Publication>>> transformedData = bxpam.transformData(publications);
        //bxpam.createTables();
        //bxpam.insertData(transformedData);
        //bxpam.populateSchema();
        //bxpam.calculateHeights();
        //EdgeModelFunctions emf = new EdgeModelFunctions(bxpam.connection);
        //emf.createFunctionXPathInEdgeModel();
        //XPathFunctions xpath = new XPathFunctions(bxpam.connection);
        //xpath.createFunctionXPath();
        //XPathSmallerWindow xPathSmallerWindow = new XPathSmallerWindow(bxpam.connection);
        //xPathSmallerWindow.createFunctionXPathSmallWindow();
        System.out.println("Benchmark prepared.");
        System.out.println("Starting Benchmark.");
        System.out.println("Benchmark On Edge Model");
        //benchmark_edge(bxpam);
        System.out.println();
        System.out.println("Benchmark On XPath Model");
        //benchmark_xpath(bxpam);
        System.out.println();
        System.out.println("Benchmark On Smaller Window & One Axis Model");
        //benchmark_xpath_sw_axis(bxpam);
        System.out.println();
        System.out.println("Benchmark my_small_bib Erweiterung");
        //benchmark_my_small_bib(args, bxpam);
    }

    public static void benchmark_edge(Berechnung_XPath_Achsen bxpam) throws SQLException {
        int countparents = 0;
        int countchilds = 0;
        Connection connection = bxpam.connection;
        long startTime = System.currentTimeMillis();
        long endTime = 0;
        Random rand = new Random();
        rand.setSeed(14932181);
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT id FROM node WHERE type = 'article'");
            while (rs.next()) {
                boolean ascending = rand.nextBoolean();
                if(ascending) {
                    int articleId = rs.getInt("id");
                    try (PreparedStatement ancestorStmt = connection.prepareStatement("SELECT * FROM get_ancestors(?)")) {
                        ancestorStmt.setInt(1, articleId);
                        try (ResultSet ancestorRs = ancestorStmt.executeQuery()) {
                            countparents++;
                            while (ancestorRs.next()) {
                                countchilds++;
                            }
                        }
                    }
                }
            }
            endTime = System.currentTimeMillis();
            System.out.println("Ancestor Edge in: " + (endTime - startTime) + " ms");
            System.out.println("P: " + countparents + " C: " + countchilds);

            countparents = 0;
            countchilds = 0;
            rs = stmt.executeQuery("SELECT id FROM node WHERE type = 'v_year'");
            while (rs.next()) {
                boolean descending = rand.nextBoolean();
                if(descending) {
                    int yearId = rs.getInt("id");
                    try (PreparedStatement descendantStmt = connection.prepareStatement("SELECT * FROM get_descendants(?)")) {
                        descendantStmt.setInt(1, yearId);
                        try (ResultSet descendantRs = descendantStmt.executeQuery()) {
                            countparents++;
                            while (descendantRs.next()) {
                                countchilds++;
                            }
                        }
                    }
                }
            }
            endTime = System.currentTimeMillis();
            System.out.println("Descendant Edge in: " + (endTime - startTime) + " ms");
            System.out.println("P: " + countparents + " C: " + countchilds);

            int count_precedent = 0;
            int count_following = 0;
            rs = stmt.executeQuery("SELECT id FROM node WHERE type = 'article'");
            while (rs.next()) {
                boolean preceding = rand.nextBoolean();
                int articleId = rs.getInt("id");
                if (preceding) {
                    try (PreparedStatement precedingStmt = connection.prepareStatement("SELECT * FROM get_preceeding_siblings(?)")) {
                        precedingStmt.setInt(1, articleId);
                        try (ResultSet precedingRs = precedingStmt.executeQuery()) {
                            while (precedingRs.next()) {
                                count_precedent++;
                            }
                        }
                    }
                } else {
                    try (PreparedStatement followingStmt = connection.prepareStatement("SELECT * FROM get_following_siblings(?)")) {
                        followingStmt.setInt(1, articleId);
                        try (ResultSet followingRs = followingStmt.executeQuery()) {
                            while (followingRs.next()) {
                                count_following++;
                            }
                        }
                    }
                }
            }
            endTime = System.currentTimeMillis();
            System.out.println("Following/Preceding Edge in: " + (endTime - startTime) + " ms");
            System.out.println("P: " + count_precedent + " F: " + count_following);


            endTime = System.currentTimeMillis();
            System.out.println("Benchmark Time for EDGE Model: " + (endTime - startTime) + " ms");
        }
    }

    public static void benchmark_xpath(Berechnung_XPath_Achsen bxpam) throws SQLException {
        Connection connection = bxpam.connection;
        int countparents = 0;
        int countchilds = 0;
        long startTime = System.currentTimeMillis();
        long endTime = 0;
        Random rand = new Random();
        rand.setSeed(14932181);
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT id FROM node WHERE type = 'article'");
            while (rs.next()) {
                boolean ascending = rand.nextBoolean();
                if(ascending) {
                    int articleId = rs.getInt("id");
                    try (PreparedStatement ancestorStmt = connection.prepareStatement("SELECT * FROM xp_ancestors(?)")) {
                        ancestorStmt.setInt(1, articleId);
                        try (ResultSet ancestorRs = ancestorStmt.executeQuery()) {
                            countparents++;
                            while (ancestorRs.next()) {
                                countchilds++;
                            }
                        }
                    }
                }
            }
            endTime = System.currentTimeMillis();
            System.out.println("Ancestor XPath in: " + (endTime - startTime) + " ms");
            System.out.println("P: " + countparents + " C: " + countchilds);

            countparents = 0;
            countchilds = 0;
            rs = stmt.executeQuery("SELECT id FROM node WHERE type = 'v_year'");
            while (rs.next()) {
                boolean descending = rand.nextBoolean();
                if(descending) {
                    int yearId = rs.getInt("id");
                    try (PreparedStatement descendantStmt = connection.prepareStatement("SELECT * FROM xp_descendants(?)")) {
                        descendantStmt.setInt(1, yearId);
                        try (ResultSet descendantRs = descendantStmt.executeQuery()) {
                            countparents++;
                            while (descendantRs.next()) {
                                countchilds++;
                            }
                        }
                    }
                }
            }
            endTime = System.currentTimeMillis();
            System.out.println("Descendant XPath in: " + (endTime - startTime) + " ms");
            System.out.println("P: " + countparents + " C: " + countchilds);

            int count_precedent = 0;
            int count_following = 0;
            rs = stmt.executeQuery("SELECT id FROM node WHERE type = 'article'");
            while (rs.next()) {
                boolean preceding = rand.nextBoolean();
                int articleId = rs.getInt("id");
                if (preceding) {
                    try (PreparedStatement precedingStmt = connection.prepareStatement("SELECT * FROM xp_prec_siblings(?)")) {
                        precedingStmt.setInt(1, articleId);
                        try (ResultSet precedingRs = precedingStmt.executeQuery()) {
                            while (precedingRs.next()) {
                                count_precedent++;
                            }
                        }
                    }
                } else {
                    try (PreparedStatement followingStmt = connection.prepareStatement("SELECT * FROM xp_fol_siblings(?)")) {
                        followingStmt.setInt(1, articleId);
                        try (ResultSet followingRs = followingStmt.executeQuery()) {
                            while (followingRs.next()) {
                                count_following++;
                            }
                        }
                    }
                }
            }

            endTime = System.currentTimeMillis();
            System.out.println("Following/Preceding XPath in: " + (endTime - startTime) + " ms");
            System.out.println("P: " + count_precedent + " F: " + count_following);


            endTime = System.currentTimeMillis();
            System.out.println("Benchmark Time for XPath Model: " + (endTime - startTime) + " ms");
        }
    }


    public static void benchmark_xpath_sw_axis(Berechnung_XPath_Achsen bxpam) throws SQLException {
        Connection connection = bxpam.connection;
        int countparents = 0;
        int countchilds = 0;
        long startTime = System.currentTimeMillis();
        long endTime = 0;
        Random rand = new Random();
        rand.setSeed(14932181);
        try (Statement stmt = connection.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT id FROM node WHERE type = 'article'");
            while (rs.next()) {
                boolean ascending = rand.nextBoolean();
                if(ascending) {
                    int articleId = rs.getInt("id");
                    try (PreparedStatement ancestorStmt = connection.prepareStatement("SELECT * FROM sw_ascending(?)")) {
                        ancestorStmt.setInt(1, articleId);
                        try (ResultSet ancestorRs = ancestorStmt.executeQuery()) {
                            countparents++;
                            while (ancestorRs.next()) {
                                countchilds++;
                            }
                        }
                    }
                }
            }
            endTime = System.currentTimeMillis();
            System.out.println("Ancestors SW in: " + (endTime - startTime) + " ms");
            System.out.println("P: " + countparents + " C: " + countchilds);

            countparents = 0;
            countchilds = 0;
            int count_axis_parents = 0;
            int count_axis_children = 0;
            rs = stmt.executeQuery("SELECT id FROM node WHERE type = 'v_year'");
            while (rs.next()) {
                boolean descending = rand.nextBoolean();
                if(descending) {
                    int yearId = rs.getInt("id");
                    try (PreparedStatement descendantStmt = connection.prepareStatement("SELECT * FROM sw_descending(?)")) {
                        descendantStmt.setInt(1, yearId);
                        try (ResultSet descendantRs = descendantStmt.executeQuery()) {
                            countparents++;
                            while (descendantRs.next()) {
                                countchilds++;
                            }
                        }
                    }
                }
            }
            endTime = System.currentTimeMillis();
            System.out.println("Descendant SW in: " + (endTime - startTime) + " ms");
            System.out.println("P: " + countparents + " C: " + countchilds);


            rs = stmt.executeQuery("SELECT id FROM node WHERE type = 'v_year'");
            while (rs.next()) {
                boolean descending = rand.nextBoolean();
                if (descending) {
                    int yearId = rs.getInt("id");
                    try (PreparedStatement descendantStmt = connection.prepareStatement("SELECT * FROM one_axis_descending(?)")) {
                        descendantStmt.setInt(1, yearId);
                        try (ResultSet descendantRs = descendantStmt.executeQuery()) {
                            count_axis_parents++;
                            while (descendantRs.next()) {
                                count_axis_children++;
                            }
                        }
                    }
                }
            }
            endTime = System.currentTimeMillis();
            System.out.println("Descendant OneAxis in: " + (endTime - startTime) + " ms");
            System.out.println("A_P " + count_axis_parents + " A_C " + count_axis_children);


            int count_precedent = 0;
            int count_following = 0;
            rs = stmt.executeQuery("SELECT id FROM node WHERE type = 'article'");
            while (rs.next()) {
                boolean preceding = rand.nextBoolean();
                int articleId = rs.getInt("id");
                if (preceding) {
                    try (PreparedStatement precedingStmt = connection.prepareStatement("SELECT * FROM sw_preceding_siblings(?)")) {
                        precedingStmt.setInt(1, articleId);
                        try (ResultSet precedingRs = precedingStmt.executeQuery()) {
                            while (precedingRs.next()) {
                                count_precedent++;
                            }
                        }
                    }
                } else {
                    try (PreparedStatement followingStmt = connection.prepareStatement("SELECT * FROM sw_following_siblings(?)")) {
                        followingStmt.setInt(1, articleId);
                        try (ResultSet followingRs = followingStmt.executeQuery()) {
                            while (followingRs.next()) {
                                count_following++;
                            }
                        }
                    }
                }
            }
            endTime = System.currentTimeMillis();
            System.out.println("Following/Preceding SW: " + (endTime - startTime) + " ms");
            System.out.println("P: " + count_precedent + " F: " + count_following);



            endTime = System.currentTimeMillis();
            System.out.println("Benchmark Time for SW_AXIS Model: " + (endTime - startTime) + " ms");
        }
    }

    public static void benchmark_my_small_bib(String[] args, Berechnung_XPath_Achsen bxpam) throws SQLException, ClassNotFoundException {
        try {
            long startTime = System.nanoTime();
            String venues = "sigmod,vldb,icde";
            bxpam.preprocessXMLFile(args[0], "src/db/my_small_bib.xml", venues);
            Path xmlPath = Paths.get("src/db/my_small_bib.xml");

            long fileSize = Files.size(xmlPath);
            System.out.println("my_small_bib.xml-Dateigröße: " + fileSize + " Bytes");
            long endTime = System.nanoTime();
            long duration = (endTime - startTime);  // compute the duration in nanoseconds
            double durationInSeconds = duration / 1_000_000_000.0;  // convert to seconds
            System.out.println("Execution time of my_small_bib: " + durationInSeconds + " seconds\n");

            System.out.println("----------------------------------------\n");
            long startTime1 = System.nanoTime();

            String venues1 = "sigmod,vldb,icde,igarss,vision,imcl";
            bxpam.preprocessXMLFile(args[0], "src/db/my_small_bib_processed1.xml", venues1);
            Path xmlPath1 = Paths.get("src/db/my_small_bib_processed1.xml");
            long fileSize1 = Files.size(xmlPath1);
            System.out.println("my_small.bib_processed-Dateigröße: " + fileSize1 + " Bytes");

            long endTime1 = System.nanoTime();
            long duration1 = (endTime1 - startTime1);  // compute the duration in nanoseconds
            double durationInSeconds1 = duration1 / 1_000_000_000.0;  // convert to seconds
            System.out.println("Execution time: " + durationInSeconds1 + " seconds");

            System.out.println("----------------------------------------\n");
            long startTime2 = System.nanoTime();

            String venues2 = "sigmod,vldb,icde,igarss,ijcon,vision,imcl,meco,IEEEants,tac,jossac";
            bxpam.preprocessXMLFile(args[0], "src/db/my_small_bib_processed2.xml", venues2);

            Path xmlPath2 = Paths.get("src/db/my_small_bib_processed2.xml");
            long fileSize2 = Files.size(xmlPath2);
            System.out.println("my_small.bib_processed-Dateigröße: " + fileSize2 + " Bytes");

            long endTime2 = System.nanoTime();
            long duration2 = (endTime2 - startTime2);  // compute the duration in nanoseconds
            double durationInSeconds2 = duration2 / 1_000_000_000.0;  // convert to seconds
            System.out.println("Execution time: " + durationInSeconds2 + " seconds");

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
