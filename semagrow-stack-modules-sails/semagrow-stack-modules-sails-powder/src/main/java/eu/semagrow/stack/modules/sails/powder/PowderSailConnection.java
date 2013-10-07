
package eu.semagrow.stack.modules.sails.powder;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.Namespace;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.UpdateExpr;
import org.openrdf.query.algebra.evaluation.TripleSource;
import org.openrdf.sail.Sail;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public class PowderSailConnection implements SailConnection, TripleSource {

    private final PowderSail powderSail;

    public PowderSailConnection(PowderSail powderSail) {
        this.powderSail = powderSail;
    }
            
    public boolean isOpen() throws SailException {
        return true;
    }

    public void close() throws SailException {        
    }

    public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(TupleExpr te, Dataset dtst, BindingSet bs, boolean bln) throws SailException {
        return null;
    }

    public void executeUpdate(UpdateExpr ue, Dataset dtst, BindingSet bs, boolean bln) throws SailException {        
    }

    public CloseableIteration<? extends Resource, SailException> getContextIDs() throws SailException {
        return null;
    }

    public CloseableIteration<? extends Statement, SailException> getStatements(Resource rsrc, URI uri, Value value, boolean bln, Resource... rsrcs) throws SailException {
        return null;
    }

    public long size(Resource... rsrcs) throws SailException {
        return 1L;
    }

    public void commit() throws SailException {
        
    }

    public void rollback() throws SailException {
        
    }

    public void addStatement(Resource rsrc, URI uri, Value value, Resource... rsrcs) throws SailException {
        
    }

    public void removeStatements(Resource rsrc, URI uri, Value value, Resource... rsrcs) throws SailException {
        
    }

    public void clear(Resource... rsrcs) throws SailException {
        
    }

    public CloseableIteration<? extends Namespace, SailException> getNamespaces() throws SailException {
        return null;
    }

    public String getNamespace(String string) throws SailException {
        return null;
    }

    public void setNamespace(String string, String string1) throws SailException {
        
    }

    public void removeNamespace(String string) throws SailException {
        
    }

    public void clearNamespaces() throws SailException {
        
    }

    public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource rsrc, URI uri, Value value, Resource... rsrcs) throws QueryEvaluationException {
        return null;
    }

    public ValueFactory getValueFactory() {
        return this.powderSail.getValueFactory();
    }

}
