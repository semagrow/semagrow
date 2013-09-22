
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
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;

/**
 *
 * @author http://www.turnguard.com/turnguard
 */
public class PowderSailConnection implements SailConnection, TripleSource {

    public boolean isOpen() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void close() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(TupleExpr te, Dataset dtst, BindingSet bs, boolean bln) throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void executeUpdate(UpdateExpr ue, Dataset dtst, BindingSet bs, boolean bln) throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public CloseableIteration<? extends Resource, SailException> getContextIDs() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public CloseableIteration<? extends Statement, SailException> getStatements(Resource rsrc, URI uri, Value value, boolean bln, Resource... rsrcs) throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public long size(Resource... rsrcs) throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void commit() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void rollback() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void addStatement(Resource rsrc, URI uri, Value value, Resource... rsrcs) throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void removeStatements(Resource rsrc, URI uri, Value value, Resource... rsrcs) throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void clear(Resource... rsrcs) throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public CloseableIteration<? extends Namespace, SailException> getNamespaces() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getNamespace(String string) throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void setNamespace(String string, String string1) throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void removeNamespace(String string) throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void clearNamespaces() throws SailException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public CloseableIteration<? extends Statement, QueryEvaluationException> getStatements(Resource rsrc, URI uri, Value value, Resource... rsrcs) throws QueryEvaluationException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public ValueFactory getValueFactory() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
