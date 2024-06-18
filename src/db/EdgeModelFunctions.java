package db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class EdgeModelFunctions {
    Connection connection;

    EdgeModelFunctions(Connection connection) {
        this.connection = connection;
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
