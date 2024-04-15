package db;

import java.sql.Connection;
import java.sql.DriverManager;

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

                connectDB.generate(num_tuples, sparsity, num_attributes, "H");

                connectDB.generateToyBsp(num_tuples, "H");
                connectDB.h2v("H", "V");
                connectDB.v2h("V", "V2H");



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
}
