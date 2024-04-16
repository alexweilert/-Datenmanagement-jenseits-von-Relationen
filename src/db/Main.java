package db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Main {


    public static void main(String[] args) {
        int num_tuples, num_attributes;
        double sparsity;

        if (args.length > 0 && args.length < 3) {
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
                else {
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
        connectDB.h2v("h", "h2v");
        connectDB.v2h("h2v", "v2h");
        System.out.println("Phase 2: h2v && v2h generated");
        System.out.println("\n");
        System.out.println("Phase 2: Benchmarks enrolling");

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("Phase 2: Extending num_attributes");
        int[] numAttributesValues = {5, 50, 100, 250, 500};

        for (int num_attributes : numAttributesValues) {
            benchmark(connectDB, 5, 0.5, num_attributes, "att" + num_attributes, "att" + num_attributes + "_to_vertical", "att" + num_attributes + "_to_horizontal");
        }

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("Phase 2: Extending num_tuples");
        int[] numTuplesValues = {5, 50, 500, 1000, 1500, 1599};

        for (int num_tuples : numTuplesValues) {
            benchmark(connectDB, num_tuples, 0.5, 1000, "tup" + num_tuples, "tup" + num_tuples + "_to_vertical", "tup" + num_tuples + "_to_horizontal");
        }

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("Phase 2: Extending sparsity");
        int[] numSparsityValues = {2, 3, 5, 7, 9};
        for (int num_sparsity : numSparsityValues) {
            if(num_sparsity > 1) {
                benchmark(connectDB, 5, Math.pow(2, -num_sparsity), 1000, "spar" + num_sparsity, "spar" + num_sparsity + "_to_vertical", "spar" + num_sparsity + "_to_horizontal");
            }
        }
    }

    public static void benchmark(ConnectDB connectDB, int num_tuples, double sparsity, int num_attributes, String create_table, String create_vertical, String create_horizontal) throws SQLException {
        long startTime, verticalTime, horizontalTime;
        System.out.println("num_tuples = " + num_tuples + ", sparsity = " + sparsity + ", num_attributes = " + num_attributes);

        //Prints storage size of each table
        connectDB.printStorageSize(create_table);

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
