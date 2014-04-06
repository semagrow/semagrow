package eu.semagrow.stack.modules.sails.semagrow;

import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailBase;

import java.io.File;

/**
 * Created by angel on 3/12/14.
 */
public class SemagrowSail extends SailBase {


    @Override
    protected void shutDownInternal() throws SailException {

    }

    public boolean isWritable() throws SailException {
        return false;
    }

    @Override
    public SailConnection getConnectionInternal() throws SailException {
        return new SemagrowConnection(this);
    }

    public ValueFactory getValueFactory() {
        return ValueFactoryImpl.getInstance();
    }
}
