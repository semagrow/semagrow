
package eu.semagrow.stack.modules.sails.powder.config;

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
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public SailImplConfig getConfig() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public Sail getSail(SailImplConfig sic) throws SailConfigException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
