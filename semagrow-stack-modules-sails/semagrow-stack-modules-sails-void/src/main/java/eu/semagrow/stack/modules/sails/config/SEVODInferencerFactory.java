package eu.semagrow.stack.modules.sails.config;

import eu.semagrow.stack.modules.sails.VOID.SEVODInferencer;
import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailImplConfig;

/**
 * Created by angel on 7/4/14.
 */
public class SEVODInferencerFactory implements SailFactory {

    public static final String SAIL_TYPE = "semagrow:SEVODInferencer";

    public String getSailType() { return SAIL_TYPE; }

    public SailImplConfig getConfig() {
        return new SEVODInferencerConfig();
    }

    public Sail getSail(SailImplConfig sailImplConfig) throws SailConfigException {
        return new SEVODInferencer();
    }
}
