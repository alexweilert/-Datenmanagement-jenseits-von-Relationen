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

    private void createFollowingSiblingsXPath() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
            "CREATE OR REPLACE FUNCTION xp_following_siblings(v INT) " +
                    "RETURNS TABLE(following_sibling_id INT) AS $$ " +
                    "DECLARE post_v INT; " +
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
            "CREATE OR REPLACE FUNCTION xp_preceding_siblings(v INT) " +
                    "RETURNS TABLE(preceding_sibling_id INT) AS $$ " +
                    "DECLARE post_v INT; " +
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

    private void createAncestorsXPath() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION xp_ancestors(v INT) " +
                            "RETURNS TABLE(ancestor_id INT) AS $$ " +
                            "DECLARE post_v INT; " +
                            "BEGIN " +
                            "SELECT post INTO post_v FROM accel WHERE id = v; " +
                            "IF post_v IS NULL THEN " +
                            "RETURN; END IF; " +
                            "RETURN QUERY " +
                            "SELECT id FROM accel WHERE id < v AND post > post_v; " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createDescendantsXPath() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION xp_descendants(v INT) " +
                            "RETURNS TABLE(descendant_id INT) AS $$ " +
                            "DECLARE post_v INT; " +
                            "BEGIN " +
                            "SELECT post INTO post_v FROM accel WHERE id = v; " +
                            "IF post_v IS NULL THEN " +
                            "RETURN; END IF; " +
                            "RETURN QUERY " +
                            "SELECT id FROM accel WHERE id > v AND post_v > post; " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
