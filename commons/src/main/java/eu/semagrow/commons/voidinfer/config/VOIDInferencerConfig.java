package eu.semagrow.commons.voidinfer.config;

import org.eclipse.rdf4j.sail.config.AbstractDelegatingSailImplConfig;
import org.eclipse.rdf4j.sail.config.SailImplConfig;

/**
 * Created by angel on 5/29/14.
 */
public class VOIDInferencerConfig extends AbstractDelegatingSailImplConfig {

    public VOIDInferencerConfig() { }

    public VOIDInferencerConfig(SailImplConfig baseConfig) {
            super(VOIDInferencerFactory.SAIL_TYPE, baseConfig);
    }
}
