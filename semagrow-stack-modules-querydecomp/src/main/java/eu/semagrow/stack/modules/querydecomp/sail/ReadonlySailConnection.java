package eu.semagrow.stack.modules.querydecomp.sail;

import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.sail.SailException;
import org.openrdf.sail.SailReadOnlyException;
import org.openrdf.sail.helpers.SailBase;
import org.openrdf.sail.helpers.SailConnectionBase;


/**
 * Created by angel on 3/13/14.
 */
public abstract class ReadonlySailConnection extends SailConnectionBase {


    public ReadonlySailConnection(SailBase sailBase){
        super(sailBase);
    }

    @Override
    protected void addStatementInternal(Resource resource, URI uri, Value value, Resource... resources) throws SailException {
        throw new SailReadOnlyException("");
    }

    @Override
    protected void removeStatementsInternal(Resource resource, URI uri, Value value, Resource... resources) throws SailException {
        throw new SailReadOnlyException("");
    }

    @Override
    protected void clearInternal(Resource... resources) throws SailException {
        throw new SailReadOnlyException("");
    }

    @Override
    protected void setNamespaceInternal(String s, String s2) throws SailException {
        throw new SailReadOnlyException("");
    }

    @Override
    protected void removeNamespaceInternal(String s) throws SailException {
        throw new SailReadOnlyException("");
    }

    @Override
    protected void clearNamespacesInternal() throws SailException {
        throw new SailReadOnlyException("");
    }
}
