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
        createSmWinDescendants();
    }

    private void createFollowingSiblingsSmallerWindow() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION smwin_fol_siblings(v INT) " +
                            "RETURNS TABLE(following_sibling_id INT) AS $$ " +
                            "DECLARE post_v INT; " +
                            "        parent_v INT; " +
                            "BEGIN " +
                            "SELECT post, parent INTO post_v, parent_v FROM accel WHERE id = v; " +
                            "IF post_v IS NULL THEN " +
                            "RETURN; END IF; " +
                            "RETURN QUERY " +
                            "SELECT id AS following_sibling_id FROM accel WHERE id > v AND parent = parent_v AND id <= post_v; " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createPrecedingSiblingsSmallerWindow() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION smwin_prec_siblings(v INT) " +
                            "RETURNS TABLE(preceding_sibling_id INT) AS $$ " +
                            "DECLARE post_v INT; " +
                            "        parent_v INT; " +
                            "BEGIN " +
                            "SELECT post, parent INTO post_v, parent_v FROM accel WHERE id = v; " +
                            "IF post_v IS NULL THEN " +
                            "RETURN; END IF; " +
                            "RETURN QUERY " +
                            "SELECT id AS preceding_sibling_id FROM accel WHERE id < v AND parent = parent_v AND post >= post_v; " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createAncestorsSmallerWindow() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION smwin_ancestors(v INT) " +
                            "RETURNS TABLE(ancestor_id INT) AS $$ " +
                            "DECLARE post_v INT; " +
                            "DECLARE pre_v INT; " +
                            "BEGIN " +
                            "SELECT id, post INTO pre_v, post_v FROM accel WHERE id = v; " +
                            "IF post_v IS NULL THEN " +
                            "RETURN; END IF; " +
                            "RETURN QUERY " +
                            "SELECT id AS ancestor_id FROM accel WHERE id < pre_v AND post > post_v; " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createDescendantsSmallerWindow() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION smwin_descendants(v INT) " +
                            "RETURNS TABLE(descendant_id INT) AS $$ " +
                            "DECLARE post_v INT; " +
                            "DECLARE pre_v INT; " +
                            "BEGIN " +
                            "   SELECT id, post INTO pre_v, post_v FROM accel WHERE id = v; " +
                            "   IF post_v IS NULL THEN " +
                            "   RETURN; END IF; " +
                            "   RETURN QUERY " +
                            "   SELECT id AS descendant_id FROM accel WHERE id > pre_v AND post <= post_v AND id <= post_v; " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void createSmWinDescendants() {
        try (Statement statement = this.connection.createStatement()) {
            statement.execute(
                    "CREATE OR REPLACE FUNCTION smwin_descendants(v INT) " +
                            "RETURNS TABLE(descendant_id INT) AS $$ " +
                            "DECLARE post_v INT; " +
                            "DECLARE pre_v INT; " +
                            "BEGIN " +
                            "   SELECT id, post INTO pre_v, post_v FROM accel WHERE id = v; " +
                            "   IF post_v IS NULL THEN " +
                            "   RETURN; END IF; " +
                            "   RETURN QUERY " +
                            "   SELECT id AS descendant_id FROM accel WHERE id > pre_v AND post <= post_v AND id <= (pre_v + post_v)/2; " +
                            "END; $$ LANGUAGE plpgsql;"
            );
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
