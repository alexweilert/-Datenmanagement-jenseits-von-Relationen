package db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class XPathFunctions {
    Connection connection;

    XPathFunctions(Connection connection){
        this.connection = connection;
    }

    public void createFunctionXPath() {
        createAncestorsXPath();
        createDescendantsXPath();
        createFollowingSiblingsXPath();
        createPrecedingSiblingsXPath();
    }

    private void createAncestorsXPath() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
            "CREATE OR REPLACE FUNCTION get_ancestors(v INT) " +
                    "RETURNS TABLE(ancestor_id INT) AS $$ " +
                    "BEGIN " +
                    "RETURN QUERY " +
                    "WITH RECURSIVE ancestors AS ( " +
                    "    SELECT parent " +
                    "    FROM accel " +
                    "    WHERE id = v " +
                    "    UNION ALL " +
                    "    SELECT a.parent " +
                    "    FROM accel a " +
                    "    JOIN ancestors ans ON a.id = ans.parent " +
                    ") " +
                    "SELECT parent AS ancestor_id FROM ancestors WHERE parent != 0; " +
                    "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createDescendantsXPath() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
            "CREATE OR REPLACE FUNCTION get_descendants(v INT) " +
                    "RETURNS TABLE(descendant_id INT) AS $$ " +
                    "BEGIN " +
                    "RETURN QUERY " +
                    "WITH RECURSIVE descendants AS ( " +
                    "    SELECT id " +
                    "    FROM accel " +
                    "    WHERE parent = v " +
                    "    UNION ALL " +
                    "    SELECT a.id " +
                    "    FROM accel a " +
                    "    JOIN descendants des ON a.parent = des.id " +
                    ") " +
                    "SELECT id AS descendant_id FROM descendants; " +
                    "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createFollowingSiblingsXPath() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
            "CREATE OR REPLACE FUNCTION get_following_siblings(v INT) " +
                    "RETURNS TABLE(following_sibling_id INT) AS $$ " +
                    "BEGIN " +
                    "RETURN QUERY " +
                    "SELECT n2.id AS following_sibling_id " +
                    "FROM accel n1 " +
                    "JOIN accel n2 ON n1.parent = n2.parent " +
                    "WHERE n1.id = v AND n2.id > n1.id; " +
                    "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createPrecedingSiblingsXPath() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
            "CREATE OR REPLACE FUNCTION preceding_siblingsXP(v INT) " +
                    "RETURNS TABLE(preceding_sibling_id INT) AS $$ " +
                    "BEGIN " +
                    "RETURN QUERY " +
                    "SELECT n2.id AS preceding_sibling_id " +
                    "FROM accel n1 " +
                    "JOIN accel n2 ON n1.parent = n2.parent " +
                    "WHERE n1.id = v AND n2.id < n1.id; " +
                    "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
