package db;

import java.sql.SQLException;

public class Sparsity_Main {

    public static void sparsity_in_ecommerce(String[] args) {
        int num_attributes, num_tuples;
        double sparsity;
        if (args.length > 0 && args.length < 3) {
            System.err.println("Usage: <num_attributes>, sparsity, num_tuples");
        }

        if (args.length == 0) {
            num_attributes = 5;
            sparsity = 0.5;
            num_tuples = 1000;
        } else {
            num_attributes = Integer.parseInt(args[0]);
            sparsity = Double.parseDouble(args[1]);
            num_tuples = Integer.parseInt(args[2]);
        } try {
            SparsityImEcommerce sparsityImEcommerce = new SparsityImEcommerce();
            if (sparsityImEcommerce.openConnection()) {
                System.out.println("Connected");
                // Phase 1 -> Generate & Toy-Beispiel
                sparsity_phase1(sparsityImEcommerce, num_attributes, sparsity, num_tuples);

                // Phase 2 -> H2V, V2H, und Benchmark
                sparsity_phase2(sparsityImEcommerce);

                sparsityImEcommerce.closeConnection();
                if (sparsityImEcommerce.isConnectionClosed())
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

    public static void sparsity_phase1(SparsityImEcommerce sparsityImEcommerce, int num_attributes, double sparsity, int num_tuples) throws SQLException {
        System.out.println("Phase 1: Started");
        sparsityImEcommerce.generate(num_attributes, sparsity, num_tuples, "h", 0);
        sparsityImEcommerce.generateToyBsp(num_attributes, "h");
        System.out.println("Phase 1: Finished");
    }

    public static void sparsity_phase2(SparsityImEcommerce sparsityImEcommerce) throws SQLException {
        System.out.println("Phase 2: Started");
        sparsityImEcommerce.generate(5, 0.5, 10000, "h", 0);
        sparsityImEcommerce.generateToyBsp(5, "h");
        sparsityImEcommerce.printStorageSize("h");
        sparsityImEcommerce.h2v("toy_bsp_null", "h2v", "v2h", 0);
        sparsityImEcommerce.printStorageSize("h2v");
        sparsityImEcommerce.v2h("h2v", "v2h", 0);
        sparsityImEcommerce.printStorageSize("v2h");
        System.out.println("Phase 2: h2v && v2h generated");


        // Hier beginnen die Benchmarks -> Um unnötige Tabellen/Views zu erhalten, kann der untere Bereich auch ausgegraut werden.
        System.out.println("\n");
        System.out.println("Phase 2: Benchmarks enrolling");
        long startTime = System.nanoTime();
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("Phase 2: Extending num_tuples");
        int[] numTuplesValues = {1000, 2000, 3000, 4000, 5000, 6000, 10000, 15000, 20000, 25000, 50000, 100000};
        for (int num_tuples : numTuplesValues) {
            benchmark(sparsityImEcommerce, 5, 0.5, num_tuples, "tup" + num_tuples, "tup" + num_tuples + "_to_vertical", "tup" + num_tuples + "_to_horizontal", 10);
        }

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("Phase 2: Extending num_attributes");
        int[] numAttributesValues = {5, 10, 20, 30, 40, 50, 100, 250, 500, 750, 1000, 1250};
        for (int num_attributes : numAttributesValues) {
            benchmark(sparsityImEcommerce, num_attributes, 0.5, 1000, "attr" + num_attributes, "attr" + num_attributes + "_to_vertical", "attr" + num_attributes + "_to_horizontal", 10);
        }

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        System.out.println("Phase 2: Extending sparsity");
        int[] numSparsityValues = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
        for (int num_sparsity : numSparsityValues) {
            if(num_sparsity > 1) {
                benchmark(sparsityImEcommerce, 5, Math.pow(2, -num_sparsity), 1000, "spar" + num_sparsity, "spar" + num_sparsity + "_to_vertical", "spar" + num_sparsity + "_to_horizontal", 10);
            }
        }
        System.out.println("Phase 2: Benchmark finished with a total time of: " + (System.nanoTime() - startTime) / 1000000 + " ms or " + ((System.nanoTime() - startTime) / 1000000000)/60 + "." + ((System.nanoTime() - startTime) / 1000000000) + "min");

    }

    public static void benchmark(SparsityImEcommerce sparsityImEcommerce, int num_attributes, double sparsity, int num_tuples, String create_table, String create_vertical, String create_horizontal, long time) throws SQLException {
        long startTime, verticalTime, horizontalTime;
        System.out.println("num_attributes = " + num_attributes + ", sparsity = " + sparsity + ", num_tuples = " + num_tuples);

        startTime = System.nanoTime();
        sparsityImEcommerce.generate(num_attributes, sparsity, num_tuples, create_table, time);
        System.out.println("Table generated in: " + (System.nanoTime() - startTime) / 1000000 + " ms or " + (System.nanoTime() - startTime) / 1000000000 + " sek");
        //Prints storage size of each table
        sparsityImEcommerce.printStorageSize(create_table);

        verticalTime = System.nanoTime();
        sparsityImEcommerce.h2v(create_table, create_vertical, create_horizontal, time);
        System.out.println("Vertical table generated in: " + (System.nanoTime() - verticalTime) / 1000000 + " ms or " + (System.nanoTime() - verticalTime) / 1000000000 + " sek");
        //Prints storage size of each table
        sparsityImEcommerce.printStorageSize(create_vertical);

        horizontalTime = System.nanoTime();
        sparsityImEcommerce.v2h(create_vertical, create_horizontal, time);
        System.out.println("Horizontal table generated in: " + (System.nanoTime() - horizontalTime) / 1000000 + " ms or " + (System.nanoTime() - horizontalTime) / 1000000000 + " sek");

        System.out.println("Total duration time: " + (System.nanoTime() - startTime) / 1000000 + " ms or " + (System.nanoTime() - startTime) / 1000000000 + " sek");
        System.out.println("\n");

    }
}
