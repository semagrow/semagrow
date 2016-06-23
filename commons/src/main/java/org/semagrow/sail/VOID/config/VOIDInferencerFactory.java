package org.semagrow.sail.VOID.config;

import org.semagrow.sail.VOID.VOIDInferencer;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;

/**
 * Created by angel on 5/29/14.
 */
public class VOIDInferencerFactory implements SailFactory {

    public static final String SAIL_TYPE = "semagrow:VOIDInferencer";

    public String getSailType() { return SAIL_TYPE; }

    public SailImplConfig getConfig() {
        return new VOIDInferencerConfig();
    }

    public Sail getSail(SailImplConfig sailImplConfig) throws SailConfigException {
        return new VOIDInferencer();
    }
}
