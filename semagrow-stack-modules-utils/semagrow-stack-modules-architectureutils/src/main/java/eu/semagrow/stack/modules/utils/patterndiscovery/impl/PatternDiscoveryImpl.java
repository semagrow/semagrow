/**
 * 
 */
package eu.semagrow.stack.modules.utils.patterndiscovery.impl;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import eu.semagrow.stack.modules.utils.patterndiscovery.PatternDiscovery;
import eu.semagrow.stack.modules.api.transformation.EquivalentURI;


/* (non-Javadoc)
 * @see eu.semagrow.stack.modules.utils.patterndiscovery.PatternDiscovery
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
		 * @see eu.semagrow.stack.modules.utils.patterndiscovery.PatternDiscovery#retrieveEquivalentPatterns()
		 */	
	public List<EquivalentURI> retrieveEquivalentPatterns() throws IOException, ClassNotFoundException, SQLException {
		Logger.getLogger(PatternDiscoveryImpl.class.getName()).log(Level.INFO, "starting Pattern Discovery for URI: " + this.uri.toString());
            List<EquivalentURI> list = new ArrayList<EquivalentURI>();
            ValueFactory valueFactory = new ValueFactoryImpl();
            //also add the original URI to the EquivalentURI list, with proximity -1 and schema null. TODO:check if this is OK with the exception handling.
            if (this.uri.toString().equals("http://semagrow.eu/schemas/t4f#precipitation")) {
                EquivalentURI equri = new EquivalentURIImpl(valueFactory.createURI("http://ontologies.seamless-ip.org/farm.owl#rainfall"), 1000, valueFactory.createURI("http://ontologies.seamless-ip.org/farm.owl"));
                list.add(equri);
                EquivalentURI equri2 = new EquivalentURIImpl(valueFactory.createURI("http://ontologies.seamless-ip.org/farm.owl#rainfallMin"), 700, valueFactory.createURI("http://ontologies.seamless-ip.org/farm.owl"));
                list.add(equri2);
                EquivalentURI equri3 = new EquivalentURIImpl(valueFactory.createURI("http://ontologies.seamless-ip.org/farm.owl#rainfallMin"), 700, valueFactory.createURI("http://ontologies.seamless-ip.org/farm.owl"));
                list.add(equri3);
            }
            if (list.size() == 0) {
            	Logger.getLogger(PatternDiscoveryImpl.class.getName()).log(Level.INFO, "found 0 equivalent URIs");
            } else {
            	String logger_message_result = "";
        		for (EquivalentURI equivalentURI : list) {
        			logger_message_result += "found equivalent URI " + equivalentURI.getEquivalent_URI().toString() + " with proximity " + equivalentURI.getProximity() + " and schema " + equivalentURI.getSchema() + "\n";
        		}
            	Logger.getLogger(PatternDiscoveryImpl.class.getName()).log(Level.INFO, logger_message_result);
            }
            EquivalentURI eq_original = new EquivalentURIImpl(uri, -1, null);
            list.add(eq_original);
            /*
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
            */
            return list;
	}
}
