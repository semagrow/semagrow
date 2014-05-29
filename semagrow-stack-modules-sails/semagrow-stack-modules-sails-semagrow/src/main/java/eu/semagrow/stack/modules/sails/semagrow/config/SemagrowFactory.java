package eu.semagrow.stack.modules.sails.semagrow.config;

import eu.semagrow.stack.modules.sails.semagrow.SemagrowSail;
import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailImplConfig;
import org.openrdf.sail.config.SailRegistry;
import org.openrdf.sail.memory.MemoryStore;

/**
 * Created by angel on 5/29/14.
 */
public class SemagrowFactory implements SailFactory {

    public static final String SAIL_TYPE = "semagrow:SemagrowSail";

    public String getSailType() {
        return SAIL_TYPE;
    }

    public SailImplConfig getConfig() {
        return new SemagrowConfig();
    }

    public Sail getSail(SailImplConfig sailImplConfig) throws SailConfigException {
        SemagrowSail sail = new SemagrowSail();
        // create metadata sail and attach to semagrowsail
        //Sail metadataSail = new FileReloadingMemoryStore(FILENAME);
        //metadataSail = new VOIDInferencer(metadataSail);
        return sail;
    }
}
