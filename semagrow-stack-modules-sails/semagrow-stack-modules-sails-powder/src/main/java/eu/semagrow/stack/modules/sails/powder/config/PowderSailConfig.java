
package eu.semagrow.stack.modules.sails.powder.config;

import eu.semagrow.stack.modules.vocabulary.SEMAGROW;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.util.GraphUtilException;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailImplConfigBase;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public class PowderSailConfig extends SailImplConfigBase {
    
    private final Map<URI,Value> configParams = new HashMap<URI,Value>();

    public PowderSailConfig() {
        super(SEMAGROW.SAILS.POWDER.POWDER_SAIL.stringValue());
    }
    
    public Map<URI,Value> getConfigParams() {
        return configParams;
    }
    
    @Override
    public Resource export(Graph graph) {
        Resource implNode = super.export(graph);
        Iterator iter = configParams.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry e = (Map.Entry) iter.next();
            graph.add(implNode, graph.getValueFactory().createURI(e.getKey().toString()), graph.getValueFactory().createLiteral(e.getValue().toString()));
        }
        return implNode;
    }

    @Override
    public void parse(Graph graph, Resource implNode) throws SailConfigException {
        super.parse(graph, implNode);
        try {
            Literal postgresHost = GraphUtil.getOptionalObjectLiteral(graph, implNode, SEMAGROW.SAILS.POWDER.POSTGRES_HOST);
            if (postgresHost != null) {                
                configParams.put(SEMAGROW.SAILS.POWDER.POSTGRES_HOST, postgresHost);
            } else { throw new SailConfigException(SEMAGROW.SAILS.POWDER.POSTGRES_HOST + " is required"); }
            
            Literal postgresPort = GraphUtil.getOptionalObjectLiteral(graph, implNode, SEMAGROW.SAILS.POWDER.POSTGRES_PORT);
            if (postgresPort != null) {
                configParams.put(SEMAGROW.SAILS.POWDER.POSTGRES_PORT, postgresPort);
            } else { throw new SailConfigException(SEMAGROW.SAILS.POWDER.POSTGRES_PORT + " is required"); }
            
            Literal postgresDatabase = GraphUtil.getOptionalObjectLiteral(graph, implNode, SEMAGROW.SAILS.POWDER.POSTGRES_DATABASE);
            if (postgresDatabase != null) {
                configParams.put(SEMAGROW.SAILS.POWDER.POSTGRES_DATABASE, postgresDatabase);
            } else { throw new SailConfigException(SEMAGROW.SAILS.POWDER.POSTGRES_DATABASE + " is required"); }
            
            Literal postgresUser = GraphUtil.getOptionalObjectLiteral(graph, implNode, SEMAGROW.SAILS.POWDER.POSTGRES_USER);
            if (postgresUser != null) {
                configParams.put(SEMAGROW.SAILS.POWDER.POSTGRES_USER, postgresUser);
            } else { throw new SailConfigException(SEMAGROW.SAILS.POWDER.POSTGRES_USER + " is required"); }
            
            Literal postgresPassword = GraphUtil.getOptionalObjectLiteral(graph, implNode, SEMAGROW.SAILS.POWDER.POSTGRES_PASSWORD);
            if (postgresPassword != null) {
                configParams.put(SEMAGROW.SAILS.POWDER.POSTGRES_PASSWORD, postgresPassword);
            } else { throw new SailConfigException(SEMAGROW.SAILS.POWDER.POSTGRES_PASSWORD + " is required"); }
            
        } catch (GraphUtilException ex) {
            throw new SailConfigException(ex);
        }
    }    

}
