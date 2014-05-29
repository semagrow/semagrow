package eu.semagrow.stack.modules.sails.config;

import org.openrdf.sail.config.DelegatingSailImplConfigBase;
import org.openrdf.sail.config.SailImplConfig;

/**
 * Created by angel on 5/29/14.
 */
public class VOIDInferencerConfig extends DelegatingSailImplConfigBase {

    public VOIDInferencerConfig() { }

    public VOIDInferencerConfig(SailImplConfig baseConfig) {
            super(VOIDInferencerFactory.SAIL_TYPE, baseConfig);
    }
}
