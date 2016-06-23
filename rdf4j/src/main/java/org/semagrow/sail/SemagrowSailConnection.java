package org.semagrow.sail;

import org.semagrow.model.SemagrowValueFactory;
import org.semagrow.plan.QueryDecomposer;
import org.semagrow.plan.QueryDecompositionException;
import org.semagrow.evaluation.reactor.FederatedEvaluationStrategyImpl;
import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.Dataset;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.SailReadOnlyException;
import org.eclipse.rdf4j.sail.helpers.AbstractSailConnection;
import org.reactivestreams.Publisher;
import org.semagrow.algebra.QueryRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Collection;
import java.util.Collections;

/**
 * A Semagrow Readonly Connection
 * @author acharal@iit.demokritos.gr
 */
public class SemagrowSailConnection extends AbstractSailConnection {

    private final Logger logger = LoggerFactory.getLogger(SemagrowSailConnection.class);

    private SemagrowSail semagrowSail;

    public SemagrowSailConnection(SemagrowSail sail)
    {
        super(sail);
        //queryEvaluation = sail.getQueryEvaluation();
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
                Collections.<IRI>emptySet(), Collections.<IRI>emptySet());
    }



    public final CloseableIteration<? extends BindingSet, QueryEvaluationException> evaluate(
            TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
            boolean includeInferred, boolean includeProvenance,
            Collection<IRI> includeOnlySources, Collection<IRI> excludeSources)
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
                         Collection<IRI> includeOnlySources,
                         Collection<IRI> excludeSources)
            throws SailException {

        logger.debug("Starting decomposition of " + tupleExpr.toString());

        TupleExpr decomposed = null;
        decomposed = decompose(tupleExpr, dataset, bindings, includeOnlySources, excludeSources);

        logger.debug("Query decomposed to " + decomposed.toString());
        logger.info("Decomposed query: " + decomposed.toString());

        throw new SailException("Closeableiteration is not implemented. Please use the queryhandler");
        //return evaluateOnly(decomposed, dataset, bindings, b, p);
    }

    public  Publisher<? extends BindingSet>
        evaluateReactive(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings, boolean b, boolean p,
                         Collection<IRI> includeOnlySources,
                         Collection<IRI> excludeSources)
            throws SailException
    {
        return evaluateInternalReactive(tupleExpr, dataset, bindings, b, p, includeOnlySources, excludeSources);
    }

    protected Publisher<? extends BindingSet>
        evaluateInternalReactive(TupleExpr tupleExpr,
                     Dataset dataset,
                     BindingSet bindings,
                     boolean b, boolean p,
                     Collection<IRI> includeOnlySources,
                     Collection<IRI> excludeSources)
            throws SailException
    {
        long start_time = System.currentTimeMillis();
        logger.debug("Starting decomposition of " + tupleExpr.toString());

        TupleExpr decomposed = null;

        decomposed = decompose(tupleExpr, dataset, bindings, includeOnlySources, excludeSources);
	    long end_time = System.currentTimeMillis();
	    logger.debug("Decomposition duration = " + (end_time - start_time));
        logger.debug("Query decomposed to " + decomposed.toString());

        return evaluateOnlyReactive(decomposed, dataset, bindings, b, p);
    }

    /*
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
    */

    public Publisher<? extends BindingSet>
        evaluateOnlyReactive(TupleExpr expr, Dataset dataset, BindingSet bindings, boolean b, boolean p)
            throws SailException
    {
        try {
            //QueryExecutorImpl executor = new QueryExecutorImpl();
            FederatedEvaluationStrategyImpl strategy = new FederatedEvaluationStrategyImpl(SemagrowValueFactory.getInstance());
            strategy.setBatchSize(semagrowSail.getBatchSize());
            return strategy.evaluate(expr, bindings);
        } catch(QueryEvaluationException e) {
            throw new SailException(e);
        }
    }

    public TupleExpr decompose(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings)
            throws QueryDecompositionException
    {
        return decompose(tupleExpr, dataset, bindings,
                Collections.<IRI>emptySet(), Collections.<IRI>emptySet());
    }

    public TupleExpr decompose(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings,
                               Collection<IRI> includeOnlySources, Collection<IRI> excludeSources)
    {
        QueryOptimizer optimizer = semagrowSail.getOptimizer();
        optimizer.optimize(tupleExpr, dataset, bindings);

        QueryDecomposer decomposer = semagrowSail.getDecomposer(includeOnlySources, excludeSources);
        
        // TupleExpr used here must have been instantiated by the 
        // SemagrowSailQuery constructor and be QueryRoot
        assert tupleExpr instanceof QueryRoot;
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
                    getStatementsInternal(Resource resource, IRI uri, Value value, boolean b, Resource... resources) throws SailException {
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
    protected void addStatementInternal(Resource resource, IRI uri, Value value, Resource... resources) throws SailException {

    }

    @Override
    protected void removeStatementsInternal(Resource resource, IRI uri, Value value, Resource... resources) throws SailException {

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
