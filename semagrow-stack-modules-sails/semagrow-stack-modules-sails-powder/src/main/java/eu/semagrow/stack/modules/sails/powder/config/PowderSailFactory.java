
package eu.semagrow.stack.modules.sails.powder.config;

import eu.semagrow.stack.modules.sails.powder.PowderSail;
import eu.semagrow.stack.modules.vocabulary.SEMAGROW;
import java.util.Iterator;
import java.util.Map;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailImplConfig;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public class PowderSailFactory implements SailFactory {

    public String getSailType() {
        return SEMAGROW.SAILS.POWDER.POWDER_SAIL.stringValue();
    }

    public SailImplConfig getConfig() {
        return new PowderSailConfig();
    }

    public Sail getSail(SailImplConfig config) throws SailConfigException {
        if (!SEMAGROW.SAILS.POWDER.POWDER_SAIL.stringValue().equals(config.getType())) {
            throw new SailConfigException("Invalid Sail type: " + config.getType());
        }

        PowderSail powderSail = new PowderSail();

        if (config instanceof PowderSailConfig) {
            PowderSailConfig powderSailConfig = (PowderSailConfig) config;

            Map<URI,Value> map = powderSailConfig.getConfigParams();            
            for(Map.Entry<URI,Value> entry : map.entrySet()){
                if(entry.getKey().equals(SEMAGROW.SAILS.POWDER.POSTGRES_HOST)){
                    powderSail.setPostgresHost(entry.getValue());
                }
                if(entry.getKey().equals(SEMAGROW.SAILS.POWDER.POSTGRES_PORT)){
                    powderSail.setPostgresPort(entry.getValue());
                }                
                if(entry.getKey().equals(SEMAGROW.SAILS.POWDER.POSTGRES_DATABASE)){
                    powderSail.setPostgresDatabase(entry.getValue());
                }                
                if(entry.getKey().equals(SEMAGROW.SAILS.POWDER.POSTGRES_USER)){
                    powderSail.setPostgresUser(entry.getValue());
                }                
                if(entry.getKey().equals(SEMAGROW.SAILS.POWDER.POSTGRES_PASSWORD)){
                    powderSail.setPostgresPassword(entry.getValue());
                }                
            }            
        }

        return powderSail;
    }

}
