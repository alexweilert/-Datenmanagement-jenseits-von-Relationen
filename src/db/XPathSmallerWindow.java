package db;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class XPathSmallerWindow {
    Connection connection;

    XPathSmallerWindow(Connection connection){
        this.connection = connection;
    }

    public void createFunctionXPathSmallWindow() {
        createAncestorsSmallerWindow();
        createDescendantsSmallerWindow();
        createFollowingSiblingsSmallerWindow();
        createPrecedingSiblingsSmallerWindow();
    }

    // zum testen mit 36 & 49
    private void createFollowingSiblingsSmallerWindow() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION sw_following_siblings(v INT) " +
                            "RETURNS TABLE(following_sibling_id INT) AS $$ " +
                            "DECLARE pre_v INT; " +
                            "        post_v INT; " +
                            "        parent_v INT; " +
                            "        height_v INT; " +
                            "BEGIN " +
                            "SELECT a.id, a.post, a.parent, h.height INTO pre_v, post_v, parent_v, height_v " +
                            "FROM accel a " +
                            "JOIN height h ON a.id = h.id " +
                            "WHERE a.id = v; " +
                            "IF pre_v IS NULL OR post_v IS NULL OR parent_v IS NULL OR height_v IS NULL THEN " +
                            "RETURN; END IF; " +
                            "RETURN QUERY " +
                            "SELECT a.id " +
                            "FROM accel a " +
                            "JOIN height h ON a.id = h.id " +
                            "WHERE a.post > post_v + height_v AND a.parent = parent_v; " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // zum testen mit 36 & 49
    private void createPrecedingSiblingsSmallerWindow() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION sw_preceding_siblings(v INT) " +
                            "RETURNS TABLE(preceding_sibling_id INT) AS $$ " +
                            "DECLARE pre_v INT; " +
                            "        post_v INT; " +
                            "        parent_v INT; " +
                            "        height_v INT; " +
                            "BEGIN " +
                            "SELECT a.id, a.post, a.parent, h.height INTO pre_v, post_v, parent_v, height_v " +
                            "FROM accel a " +
                            "JOIN height h ON a.id = h.id " +
                            "WHERE a.id = v; " +
                            "IF pre_v IS NULL OR post_v IS NULL OR parent_v IS NULL OR height_v IS NULL THEN " +
                            "RETURN; END IF; " +
                            "RETURN QUERY " +
                            "SELECT a.id " +
                            "FROM accel a " +
                            "JOIN height h ON a.id = h.id " +
                            "WHERE a.post < post_v - height_v AND a.parent = parent_v; " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Zum testen mit 9
    private void createAncestorsSmallerWindow() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION sw_ascending(v INT) " +
                            "RETURNS TABLE(ancestor_id INT) AS $$ " +
                            "DECLARE pre_v INT; " +
                            "        post_v INT; " +
                            "        height_v INT; " +
                            "BEGIN " +
                            "SELECT a.id, a.post, h.height INTO pre_v, post_v, height_v " +
                            "FROM accel a " +
                            "JOIN height h ON a.id = h.id " +
                            "WHERE a.id = v; " +
                            "IF pre_v IS NULL OR post_v IS NULL OR height_v IS NULL THEN " +
                            "RETURN; END IF; " +
                            "RETURN QUERY " +
                            "SELECT a.id " +
                            "FROM accel a " +
                            "JOIN height h ON a.id = h.id " +
                            "WHERE a.id <= (pre_v - height_v) AND a.post >= (post_v + height_v); " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    // Zum Testen mit 35
    private void createDescendantsSmallerWindow() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION sw_descending(v INT) " +
                            "RETURNS TABLE(descendant_id INT) AS $$ " +
                            "DECLARE pre_v INT; " +
                            "        post_v INT; " +
                            "        height_v INT; " +
                            "BEGIN " +
                            "SELECT a.id, a.post, h.height INTO pre_v, post_v, height_v " +
                            "FROM accel a " +
                            "JOIN height h ON a.id = h.id " +
                            "WHERE a.id = v; " +
                            "IF pre_v IS NULL OR post_v IS NULL OR height_v IS NULL THEN " +
                            "RETURN; END IF; " +
                            "RETURN QUERY " +
                            "WITH RECURSIVE descendant(id) AS ( " +
                            "SELECT id FROM accel WHERE parent = v " +
                            "UNION ALL " +
                            "SELECT a.id FROM accel a, descendant " +
                            "WHERE a.parent = descendant.id AND a.id <= post_v + height_v " +
                            ") " +
                            "SELECT descendant.id FROM descendant; " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
