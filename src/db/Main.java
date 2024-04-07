package db;

import java.sql.Connection;
import java.sql.DriverManager;
import db.ConnectDB;

public class Main {
    public static void main(String[] args) throws Exception{
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
            Class.forName("org.postgresql.Driver");
            Connection connection = DriverManager.getConnection("jdbc:postgresql://localhost:5432/postgres", "postgres", "alex");
            ConnectDB connectDB = new ConnectDB(connection);
            if (connection != null) {
                System.out.println("Connected");
                connectDB.generate(num_tuples, sparsity, num_attributes);
                connectDB.generateToyBsp(num_tuples);
            } else {
               System.out.println("Not Connected");
            }
        } catch (Exception e) {
            System.out.println(e);
        }
    }
}
