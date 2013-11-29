/**
 * 
 */
package eu.semagrow.stack.modules.utils.impl;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import eu.semagrow.stack.modules.utils.EquivalentURI;
import eu.semagrow.stack.modules.utils.PatternDiscovery;

/* (non-Javadoc)
 * @see eu.semagrow.stack.modules.utils.PatternDiscovery
 */	
public class PatternDiscoveryImpl implements PatternDiscovery {
	
	private URI uri;
	/**
	 * @param uri the URI for which equivalent expressions are asked
	 */
	public PatternDiscoveryImpl(URI uri) {
		super();
		this.uri = uri;
	}
        /* (non-Javadoc)
		 * @see eu.semagrow.stack.modules.utils.PatternDiscovery#retrieveEquivalentPatterns()
		 */	
	public ArrayList<EquivalentURI> retrieveEquivalentPatterns() throws IOException, ClassNotFoundException, SQLException {
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
            	
            	ValueFactory valueFactory = new ValueFactoryImpl();
            	
	            Class.forName("org.postgresql.Driver");
	            String url = "jdbc:postgresql://" + host + ":" + port + "/" + dbName + "";
	            
	            connection = DriverManager.getConnection(url,user, password);
	            Statement stmt = connection.createStatement();
	            
	            String sql = "SELECT * FROM aligned_elements WHERE entity1_uri = '" + this.uri.toString() + "'";
	            
	            ResultSet rs = stmt.executeQuery(sql);
	            while (rs.next()){
	                String equri_raw = rs.getString("entity2_uri");
	                //String relation = rs.getString("alignment_relation");
	                double confidence = rs.getDouble("confidence");
	                String onto = rs.getString("onto2_uri");
	                int normalized = (int) (confidence*1000);
	                
	                EquivalentURI equri = new EquivalentURIImpl(valueFactory.createURI(equri_raw), normalized, valueFactory.createURI(onto));
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
