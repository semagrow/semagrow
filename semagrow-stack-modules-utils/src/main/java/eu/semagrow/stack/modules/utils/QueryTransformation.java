/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.semagrow.stack.modules.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

/**
 *
 * @author akukurik
 */
public class QueryTransformation {
    private URI uri;
	/**
	 * @param uri the URI for which equivalent expressions are asked
	 */
	public QueryTransformation(URI uri) {
		super();
		this.uri = uri;
	}
        /**
	 * @return A list of equivalent URIs aligned with a certain confidence with the initial URI and belonging to a specific schema
	 */	
	public ArrayList<EquivalentURI> retrieveEquivalentPatterns(URI schema) throws IOException, ClassNotFoundException, SQLException, URISyntaxException {
            ArrayList<EquivalentURI> list = new ArrayList<EquivalentURI>();
            
            
            Properties prop = new Properties();
            prop.load(new FileInputStream("config.db"));
        
            String host = prop.getProperty("host");
            String port = prop.getProperty("port");
            String dbName = prop.getProperty("database");
            String user = prop.getProperty("username");
            String password = prop.getProperty("password");
            
            Connection connection = null;
            try {
	            Class.forName("org.postgresql.Driver");
	            String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbName + "";
	            //System.out.println(url);
	            connection = DriverManager.getConnection(url,user, password);
	            Statement stmt = connection.createStatement();
	            
	            String sql = "SELECT * FROM aligned_elements WHERE entity1_uri = '" + this.uri.toString() + "'";
	            //System.out.println(sql);
	            ResultSet rs = stmt.executeQuery(sql);
	            while (rs.next()){
	                String equri_raw = rs.getString("entity2_uri");
	                //String relation = rs.getString("alignment_relation");
	                double confidence = rs.getDouble("confidence");
	                String onto = rs.getString("onto2_uri");
	                int normalized = (int) (confidence*1000);
	                
	                EquivalentURI equri = new EquivalentURI(new URI(equri_raw), normalized, new URI(onto));
	                list.add(equri);
	            }
	            
	            stmt.close();
            } finally {
            	if (connection != null) {
            		connection.close();
            	}
            }
            
            return list;
	}
}
