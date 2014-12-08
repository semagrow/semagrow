/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.semagrow.stack.modules.alignment;

import eu.semagrow.stack.modules.api.transformation.EquivalentURI;
import eu.semagrow.stack.modules.api.transformation.QueryTransformation;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

/**
 *
 * @author akukurik
 * @author acharal
 */
public class QueryTransformationImpl implements QueryTransformation {

    private ValueFactory vf;

    private static final Logger logger = LoggerFactory.getLogger(QueryTransformationImpl.class);

    private String databaseUrl;

    private String databaseUsername;

    private String databasePassword;

	public QueryTransformationImpl(ValueFactory vf, String databaseUrl, String username, String password) {
        this.vf = vf;

        this.databaseUrl = databaseUrl;
        this.databaseUsername = username;
        this.databasePassword = password;
    }

    public QueryTransformationImpl(String databaseUrl, String username, String password) {
        this(ValueFactoryImpl.getInstance(), databaseUrl, username, password);
    }
        
    /**
	* @return A list of equivalent URIs aligned with a certain confidence with the initial URI and belonging to a specific schema
	*/
    public Collection<EquivalentURI> retrieveEquivalentURIs(URI uri)
    {
        Collection<EquivalentURI> list = new LinkedList<EquivalentURI>();

        Connection connection = null;
        Statement stmt = null;

        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(databaseUrl, databaseUsername, databasePassword);

            stmt = connection.createStatement();

            String sql = "SELECT * FROM aligned_elements WHERE entity1_uri = '" + uri.toString() + "'";

            logger.debug(sql);

            ResultSet rs = stmt.executeQuery(sql);
            while (rs.next()) {
                String equri_raw = rs.getString("entity2_uri");
                String relation = rs.getString("alignment_relation");

                double confidence = rs.getDouble("confidence");

                String onto2 = rs.getString("onto2_uri");
                String onto1 = rs.getString("onto1_uri");

                int normalized = (int) (confidence * 1000);
                int transformationID = rs.getInt("id");

                EquivalentURI equri = createEquivalentURI(uri, vf.createURI(equri_raw), vf.createURI(onto1), vf.createURI(onto2), normalized, transformationID);

                list.add(equri);
            }
        } catch(SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();

                if (connection != null)
                    connection.close();
            } catch (SQLException e) { }
        }


        return list;
    }

    /**
     *
     * @param source the URI of the resource for which an equivalent is asked
     * @param transformationID the id of the specific transformation that produced the equivalence
     * @return A URI that is equivalent to the source URI under transformationID
     * @throws java.io.IOException
     * @throws ClassNotFoundException
     * @throws java.sql.SQLException
     * @throws java.net.URISyntaxException
     */
    public URI getURI(URI source, int transformationID) {

        URI response = null;

        String equri_raw = null;

        double confidence;

        int normalized;

        Connection connection = null;
        Statement stmt = null;

        String sql = "SELECT * FROM aligned_elements WHERE entity1_uri = '" + source.toString() + "' AND id=" + transformationID;

        logger.debug(sql);

        try {
            try {
                Class.forName("org.postgresql.Driver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

            connection = DriverManager.getConnection(databaseUrl, databaseUsername, databasePassword);
            stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                equri_raw = rs.getString("entity2_uri");
                confidence = rs.getDouble("confidence");
                normalized = (int) (confidence * 1000);
            }
        } catch (SQLException e) {
            logger.warn("Cannot execute SQL query " + sql, e);
        } finally {
            try {
                if (stmt != null)
                    stmt.close();

                if (connection != null)
                    connection.close();

            } catch (SQLException e) {
                logger.warn("Cannot close SQL connection " + sql, e);
            }
        }

        if (equri_raw == null)
            return null;

        response = vf.createURI(equri_raw);

        return response;
    }

    /**
     *
     * @param target the URI of the resource for which an equivalent is asked
     * @param transformationID the id of the specific transformation that produced the equivalence
     * @return A URI that is equivalent to the target URI under transformationID
     * @throws java.io.IOException
     * @throws ClassNotFoundException
     * @throws java.sql.SQLException
     * @throws java.net.URISyntaxException
     */
    public URI getInvURI(URI target, int transformationID) {

        URI response = null;
        String equri_raw = null;

        double confidence;
        int normalized;




        String sql = "SELECT * FROM aligned_elements WHERE entity2_uri = '" + target.toString() + "' AND id=" + transformationID;

        logger.debug(sql);
        Connection connection = null;
        Statement stmt = null;

        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(databaseUrl, databaseUsername, databasePassword);
            stmt = connection.createStatement();

            ResultSet rs = stmt.executeQuery(sql);

            while (rs.next()) {
                equri_raw = rs.getString("entity1_uri");
                confidence = rs.getDouble("confidence");
                normalized = (int) (confidence*1000);
            }
        } catch (SQLException e) {
            logger.warn("Cannot execute SQL query " + sql, e);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            try {
                if (stmt != null)
                    stmt.close();

                if (connection != null)
                    connection.close();

            } catch (SQLException e) {
                logger.warn("Cannot close SQL connection " + sql, e);
            }
        }

        if (equri_raw==null)
            return null;

        response = vf.createURI(equri_raw);

        return response;
    }

    private EquivalentURI createEquivalentURI(URI source, URI target, URI sourceSchema, URI targetSchema, int proximity, int trasformationId) {
        return new EquivalentURIImpl(source, target, sourceSchema, targetSchema, proximity, trasformationId);
    }

}