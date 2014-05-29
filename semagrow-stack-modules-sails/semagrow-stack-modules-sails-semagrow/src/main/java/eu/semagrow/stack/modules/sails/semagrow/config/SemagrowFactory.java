package eu.semagrow.stack.modules.sails.semagrow.config;

import eu.semagrow.stack.modules.sails.semagrow.SemagrowSail;
import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailImplConfig;

/**
 * Created by angel on 5/29/14.
 */
public class SemagrowFactory implements SailFactory {

    public String getSailType() {
        return null;
    }

    public SailImplConfig getConfig() {
        return new SemagrowConfig();
    }

    public Sail getSail(SailImplConfig sailImplConfig) throws SailConfigException {
        SemagrowSail sail = new SemagrowSail();
        return sail;
    }
}
