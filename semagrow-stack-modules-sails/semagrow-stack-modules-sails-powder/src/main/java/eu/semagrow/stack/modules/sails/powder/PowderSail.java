
package eu.semagrow.stack.modules.sails.powder;

import java.io.File;
import org.openrdf.model.ValueFactory;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public class PowderSail implements Sail {

    public void setDataDir(File file) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public File getDataDir() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void initialize() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void shutDown() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public boolean isWritable() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public SailConnection getConnection() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ValueFactory getValueFactory() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
