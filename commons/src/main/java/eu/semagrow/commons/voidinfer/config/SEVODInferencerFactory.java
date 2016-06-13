package eu.semagrow.commons.voidinfer.config;

import eu.semagrow.commons.voidinfer.VOID.SEVODInferencer;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;

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
