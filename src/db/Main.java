package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Main {

    
    public static void main(String[] args){
        int num_tuples, num_attributes;
        double sparsity;

        if ( args.length > 0 && args.length < 3) {
            System.err.println("Usage: <num_tuples>, sparsity, num_attributes");
        }

        if (args.length == 0) {
            num_tuples = 5;
            sparsity = 0.5;
            num_attributes = 1000;
        } else {
            num_tuples = Integer.parseInt(args[0]);
            sparsity = Double.parseDouble(args[1]);
            num_attributes = Integer.parseInt(args[2]);
        } try {
            ConnectDB connectDB = new ConnectDB();
            if (connectDB.openConnection()) {
                System.out.println("Connected");
                phase1(connectDB, num_tuples, sparsity, num_attributes);
                phase2(connectDB);

                connectDB.closeConnection();
                if (connectDB.isConnectionClosed())
                    System.out.println("Connection Closed");
                else{
                    System.out.println("Still connected");
                }
            } else {
               System.err.println("Connection not available");
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
        }
    }

    public static void phase1(ConnectDB connectDB, int num_tuples, double sparsity, int num_attributes) throws SQLException {
        System.out.println("Phase 1: Started");
        connectDB.generate(num_tuples, sparsity, num_attributes, "h");
        connectDB.generateToyBsp(num_tuples, "h");
        System.out.println("Phase 1: Finished");
    }

    public static void phase2(ConnectDB connectDB) throws SQLException {
        System.out.println("Phase 2: Started");
        connectDB.h2v("h", "v");
        connectDB.v2h("v", "v2h");
        System.out.println("Phase 2: h2v && v2h generated");
        System.out.println("\n");
        System.out.println("Phase 2: Benchmarks enrolling");

        System.out.println("Phase 2: Extending num_attributes");
        long startTime = System.nanoTime();
        System.out.println("1. num_tuples = 5, sparsity = 0.5, num_attributes = 1000");
        benchmark(connectDB, 5, 0.5, 1000, "att1000", "att1000_to_vertical", "att1000_to_horizontal");
        System.out.println("2. num_tuples = 5, sparsity = 0.5, num_attributes = 5000");
        benchmark(connectDB, 5, 0.5, 5000, "att5000", "att5000_to_vertical", "att5000_to_horizontal");
        System.out.println("3. num_tuples = 5, sparsity = 0.5, num_attributes = 10000");
        benchmark(connectDB, 5, 0.5, 10000, "att10000", "att10000_to_vertical", "att10000_to_horizontal");
        System.out.println("4. num_tuples = 5, sparsity = 0.5, num_attributes = 25000");
        benchmark(connectDB, 5, 0.5, 25000, "att25000", "att25000_to_vertical", "att25000_to_horizontal");
        System.out.println("5. num_tuples = 5, sparsity = 0.5, num_attributes = 50000");
        benchmark(connectDB, 5, 0.5, 50000, "att50000", "att50000_to_vertical", "att50000_to_horizontal");


        System.out.println("Phase 2: Extending num_tuples");
        System.out.println("1. num_tuples = 5, sparsity = 0.5, num_attributes = 1000");
        benchmark(connectDB, 5, 0.5, 1000, "tup5", "tup5_to_vertical", "tup5_to_horizontal");
        System.out.println("2. num_tuples = 50, sparsity = 0.5, num_attributes = 1000");
        benchmark(connectDB, 50, 0.5, 1000, "tup50", "tup50_to_vertical", "tup50_to_horizontal");
        System.out.println("3. num_tuples = 100, sparsity = 0.5, num_attributes = 1000");
        benchmark(connectDB, 100, 0.5, 1000, "tup100", "tup100_to_vertical", "tup100_to_horizontal");
        System.out.println("4. num_tuples = 2500, sparsity = 0.5, num_attributes = 1000");
        benchmark(connectDB, 250, 0.5, 1000, "tup250", "tup250_to_vertical", "tup250_to_horizontal");
        System.out.println("5. num_tuples = 5000, sparsity = 0.5, num_attributes = 1000");
        benchmark(connectDB, 500, 0.5, 1000, "tup500", "tup500_to_vertical", "tup500_to_horizontal");


        System.out.println("Phase 2: Extending sparsity");
        System.out.println("1. num_tuples = 5, sparsity = 0.5, num_attributes = 1000");
        benchmark(connectDB, 5, 0.5, 1000, "spar05", "spar05_to_vertical", "spar05_to_horizontal");
        System.out.println("2. num_tuples = 5, sparsity = 0.25, num_attributes = 1000");
        benchmark(connectDB, 5, 0.25, 1000, "spar025", "spar025_to_vertical", "spar025_to_horizontal");
        System.out.println("3. num_tuples = 5, sparsity = 0.75, num_attributes = 1000");
        benchmark(connectDB, 5, 0.75, 1000, "spar075", "spar075_to_vertical", "spar075_to_horizontal");
        System.out.println("3. num_tuples = 5, sparsity = 1, num_attributes = 1000");
        benchmark(connectDB, 5, 1, 1000, "spar1", "spar1_to_vertical", "spar1_to_horizontal");
        System.out.println("Benchmark finished with a duration of: " + (System.nanoTime() - startTime) / 1000000);
        System.out.println("Phase 2: Finished");
    }

    public static void benchmark(ConnectDB connectDB, int num_tuples, double sparsity, int num_attributes, String create_table, String create_vertical, String create_horizontal) throws SQLException {
        long startTime, verticalTime, horizontalTime;

        startTime = System.nanoTime();
        connectDB.generate(num_tuples, sparsity, num_attributes, create_table);
        System.out.println("Table generated in: " + (System.nanoTime() - startTime) / 1000000 + " ms or " + (System.nanoTime() - startTime) / 1000000000 + " sek");


        verticalTime = System.nanoTime();
        connectDB.h2v(create_table, create_vertical);
        System.out.println("Vertical table generated in: " + (System.nanoTime() - verticalTime) / 1000000 + " ms or " + (System.nanoTime() - verticalTime) / 1000000000 + " sek");


        horizontalTime = System.nanoTime();
        connectDB.v2h(create_vertical, create_horizontal);
        System.out.println("Horizontal table generated in: " + (System.nanoTime() - horizontalTime) / 1000000 + " ms or " + (System.nanoTime() - horizontalTime) / 1000000000 + " sek");


        System.out.println("Total duration time: " + (System.nanoTime() - startTime) / 1000000 + " ms or " + (System.nanoTime() - startTime) / 1000000000 + " sek");
        System.out.println("\n");
    }

}
