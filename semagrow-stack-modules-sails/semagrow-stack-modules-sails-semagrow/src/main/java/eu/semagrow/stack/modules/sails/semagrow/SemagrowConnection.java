package eu.semagrow.stack.modules.sails.semagrow;

import info.aduna.iteration.CloseableIteration;
import info.aduna.iteration.EmptyIteration;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.SailReadOnlyException;
import org.openrdf.sail.helpers.SailConnectionBase;

import java.util.Set;

/**
 * A Semagrow Readonly Connection
 * @author acharal@iit.demokritos.gr
 */
public class SemagrowConnection extends SailConnectionBase {

    private QueryOptimizer optimizer;

    private EvaluationStrategy evaluationStrategy;

    private SailConnection metadataConnection;

    private static final URI METADATA_GRAPH = ValueFactoryImpl.getInstance().createURI("http://www.semagrow.eu/metadata");

    public SemagrowConnection(SemagrowSail sail, SailConnection baseConn)
    {
        super(sail);
        ValueFactory vf = sail.getValueFactory();
        metadataConnection = baseConn;
        optimizer = sail.getOptimizer();
        evaluationStrategy = sail.getEvaluationStrategy();
    }

    @Override
    protected void closeInternal() throws SailException {
        metadataConnection.close();
    }

    /**
     * Evaluates a query represented as TupleExpr
     * @param tupleExpr the tuple expression to evaluate
     * @param dataset
     * @param bindings
     * @param b
     * @return the resultset as a closable iteration
     * @throws SailException
     */
    @Override
    protected CloseableIteration<? extends BindingSet, QueryEvaluationException>
                   evaluateInternal(TupleExpr tupleExpr,
                                    Dataset dataset,
                                    BindingSet bindings,
                                    boolean b) throws SailException {


        if (redirectToBase(tupleExpr, dataset, bindings, b))
            return metadataConnection.evaluate(tupleExpr, null, bindings, b);

        TupleExpr decomposed = decompose(tupleExpr, dataset, bindings);

        return new EmptyIteration<BindingSet, QueryEvaluationException>();
        /*
        try {
            return evaluationStrategy.evaluate(decomposed,bindings);

        } catch (QueryEvaluationException e) {
            throw new SailException(e);
        }
        */
    }

    public TupleExpr decompose(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {

        if (!redirectToBase(tupleExpr, dataset, bindings, false)) {
            optimizer.optimize(tupleExpr, dataset, bindings);
        }
        return tupleExpr;
    }

    protected boolean redirectToBase(TupleExpr tupleExpr,
                               Dataset dataset,
                               BindingSet bindings,
                               boolean b) {

        if (dataset != null && dataset.getDefaultGraphs() != null) {
            return dataset.getDefaultGraphs().contains(METADATA_GRAPH);
        }
        return false;
    }

    @Override
    protected CloseableIteration<? extends Resource, SailException>
                    getContextIDsInternal() throws SailException {
        return null;
    }

    @Override
    protected CloseableIteration<? extends Statement, SailException>
                    getStatementsInternal(Resource resource, URI uri, Value value, boolean b, Resource... resources) throws SailException {
        return null;
    }

    @Override
    protected long sizeInternal(Resource... resources) throws SailException {
        return 0;
    }

    @Override
    protected void startTransactionInternal() throws SailException {

    }

    @Override
    protected void commitInternal() throws SailException {

    }

    @Override
    protected void rollbackInternal() throws SailException {

    }

    @Override
    protected CloseableIteration<? extends Namespace, SailException> getNamespacesInternal() throws SailException {
        return null;
    }

    @Override
    protected String getNamespaceInternal(String s) throws SailException {
        return null;
    }


    @Override
    protected void addStatementInternal(Resource resource, URI uri, Value value, Resource... resources) throws SailException {
        //throw new SailReadOnlyException("");
        metadataConnection.addStatement(resource,uri,value,resources);
    }

    @Override
    protected void removeStatementsInternal(Resource resource, URI uri, Value value, Resource... resources) throws SailException {
        //throw new SailReadOnlyException("");
        metadataConnection.removeStatements(resource,uri,value,resources);
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
