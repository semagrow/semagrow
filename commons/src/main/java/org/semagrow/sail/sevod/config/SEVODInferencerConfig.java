package org.semagrow.sail.SEVOD.config;

import org.eclipse.rdf4j.sail.config.AbstractDelegatingSailImplConfig;
import org.eclipse.rdf4j.sail.config.SailImplConfig;

/**
 * Created by angel on 7/4/14.
 */
public class SEVODInferencerConfig extends AbstractDelegatingSailImplConfig {

    public SEVODInferencerConfig() { }

    public SEVODInferencerConfig(SailImplConfig baseConfig) {

        super(SEVODInferencerFactory.SAIL_TYPE, baseConfig);
    }
}