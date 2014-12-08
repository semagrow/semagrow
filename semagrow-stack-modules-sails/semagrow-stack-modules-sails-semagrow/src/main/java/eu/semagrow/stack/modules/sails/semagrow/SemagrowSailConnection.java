package eu.semagrow.stack.modules.sails.semagrow;

import eu.semagrow.stack.modules.api.decomposer.QueryDecomposer;
import eu.semagrow.stack.modules.api.decomposer.QueryDecompositionException;
import eu.semagrow.stack.modules.api.evaluation.*;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.EvaluationStrategyImpl;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.QueryExecutorImpl;
import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.QueryRoot;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.SailReadOnlyException;
import org.openrdf.sail.helpers.SailConnectionBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;

/**
 * A Semagrow Readonly Connection
 * @author acharal@iit.demokritos.gr
 */
public class SemagrowSailConnection extends SailConnectionBase {

    private final Logger logger = LoggerFactory.getLogger(SemagrowSailConnection.class);

    private SemagrowSail semagrowSail;

    private FederatedQueryEvaluation queryEvaluation;

    private static final URI METADATA_GRAPH =
            ValueFactoryImpl.getInstance().createURI("http://www.semagrow.eu/metadata");

    public SemagrowSailConnection(SemagrowSail sail)
    {
        super(sail);
        queryEvaluation = sail.getQueryEvaluation();
        this.semagrowSail = sail;
    }

    @Override
    protected void closeInternal() throws SailException {

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

        return evaluateInternal(tupleExpr, dataset, bindings, b, false,
                Collections.<URI>emptySet(), Collections.<URI>emptySet());
    }



    public final CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
            TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
            boolean includeInferred, boolean includeProvenance,
            Collection<URI> includeOnlySources, Collection<URI> excludeSources)
            throws SailException
    {

        //FIXME: flushPendingUpdates();
        connectionLock.readLock().lock();
        try {
            verifyIsOpen();
            boolean registered = false;
            CloseableIteration<? extends BindingSet, QueryEvaluationException> iteration =
                    evaluateInternal(tupleExpr, dataset, bindings,
                            includeInferred, includeProvenance,
                            includeOnlySources, excludeSources);
            try {
                CloseableIteration<? extends BindingSet, QueryEvaluationException> registeredIteration =
                        registerIteration(iteration);
                registered = true;
                return registeredIteration;
            }
            finally {
                if (!registered) {
                    try {
                        iteration.close();
                    }
                    catch (QueryEvaluationException e) {
                        throw new SailException(e);
                    }
                }
            }
        }
        finally {
            connectionLock.readLock().unlock();
        }
    }

    /**
     * Evaluates a query represented as TupleExpr
     * @param tupleExpr the tuple expression to evaluate
     * @param dataset
     * @param bindings
     * @param b include inferred
     * @param p include provenance
     * @return the resultset as a closable iteration
     * @throws SailException
     */
    protected CloseableIteration<? extends BindingSet, QueryEvaluationException>
        evaluateInternal(TupleExpr tupleExpr,
                         Dataset dataset,
                         BindingSet bindings,
                         boolean b, boolean p,
                         Collection<URI> includeOnlySources,
                         Collection<URI> excludeSources)
            throws SailException {

        logger.debug("Starting decomposition of " + tupleExpr.toString());

        TupleExpr decomposed = null;
        try {
            decomposed = decompose(tupleExpr, dataset, bindings, includeOnlySources, excludeSources);
        } catch (QueryDecompositionException e) {
            throw new SailException(e);
        }
        logger.debug("Query decomposed to " + decomposed.toString());
        logger.info("Decomposed query: " + decomposed.toString());

        return evaluateOnly(decomposed, dataset, bindings, b, p);
    }

    public CloseableIteration<? extends BindingSet, QueryEvaluationException>
        evaluateOnly(TupleExpr tupleExpr,
                         Dataset dataset,
                         BindingSet bindings,
                         boolean b, boolean p) throws SailException {

        try {
            logger.info("Query evaluation started.");

            FederatedQueryEvaluationSession session = queryEvaluation.createSession(tupleExpr, dataset, bindings);

            FederatedEvaluationStrategy evaluationStrategy = session.getEvaluationStrategy();

            evaluationStrategy.setIncludeProvenance(p);

            CloseableIteration<BindingSet,QueryEvaluationException> result =
                    evaluationStrategy.evaluate(tupleExpr, bindings);

            logger.info("Query evaluation completed.");

            return result;
        } catch (QueryEvaluationException e) {
            throw new SailException(e);
        }
    }

    public TupleExpr decompose(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings)
            throws QueryDecompositionException
    {
        return decompose(tupleExpr, dataset, bindings,
                Collections.<URI>emptySet(), Collections.<URI>emptySet());
    }

    public TupleExpr decompose(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
                               Collection<URI> includeOnlySources, Collection<URI> excludeSources)
            throws QueryDecompositionException {

        QueryOptimizer optimizer = semagrowSail.getOptimizer();
        optimizer.optimize(tupleExpr, dataset, bindings);
        QueryDecomposer decomposer = semagrowSail.getDecomposer(includeOnlySources, excludeSources);
        tupleExpr = new QueryRoot(tupleExpr);
        decomposer.decompose(tupleExpr, dataset, bindings);

        return tupleExpr;
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
    protected void addStatementInternal(Resource resource, URI uri, Value value, Resource... resources) throws SailException {

    }

    @Override
    protected void removeStatementsInternal(Resource resource, URI uri, Value value, Resource... resources) throws SailException {

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
