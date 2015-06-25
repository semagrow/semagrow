package eu.semagrow.commons.voidinfer.config;

import org.openrdf.sail.config.DelegatingSailImplConfigBase;
import org.openrdf.sail.config.SailImplConfig;

/**
 * Created by angel on 7/4/14.
 */
public class SEVODInferencerConfig extends DelegatingSailImplConfigBase {

    public SEVODInferencerConfig() { }

    public SEVODInferencerConfig(SailImplConfig baseConfig) {

        super(SEVODInferencerFactory.SAIL_TYPE, baseConfig);
    }
}