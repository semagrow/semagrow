package org.semagrow.connector.sparql.execution;

import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryOptimizerList;
import org.semagrow.connector.sparql.SPARQLSite;
import org.semagrow.evaluation.file.MaterializationManager;
import org.semagrow.evaluation.BindingSetOps;
import org.semagrow.evaluation.util.BindingSetUtil;
import org.semagrow.evaluation.util.LoggingUtil;
import org.semagrow.evaluation.util.SimpleBindingSetOps;
import org.semagrow.evaluation.QueryExecutor;
import org.semagrow.connector.sparql.query.render.SPARQLQueryStringUtil;
import org.semagrow.geospatial.execution.BBoxDistanceOptimizer;
import org.semagrow.selector.Site;
import org.semagrow.querylog.api.QueryLogHandler;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.TupleExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.net.URL;
import java.util.*;

/**
 * Reactor Streams Query Executor
 *
 * <p>Implementation of QueryExecutor interface. The implementation uses Reactor Streams library.
 *
 * @author Antonis Troumpoukis
 */

public class SPARQLQueryExecutor extends ConnectionManager implements QueryExecutor
{
    private boolean rowIdOpt = false;
    private QueryLogHandler qfrHandler;
    private MaterializationManager mat;

    protected BindingSetOps bindingSetOps = SimpleBindingSetOps.getInstance();

    public SPARQLQueryExecutor(QueryLogHandler qfrHandler, MaterializationManager mat) {
        this.qfrHandler = qfrHandler;
        this.mat = mat;
    }

    /**
     * Evaluation of a remote query to a specified endpoint, given a binding.
     *
     * @param endpoint The endpoint in which the source query is to be sent
     * @param expr The tuple expression that corresponds to the source query
     * @param bindings a list of bindings
     * @return A publisher who publishes the evaluation results
     * @throws QueryEvaluationException
     */
    public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException
    {
        optimize(expr, bindings); // FIXME
        return evaluateReactorImpl((SPARQLSite)endpoint, expr, bindings);
    }

    /**
     * Evaluation of a remote query to a specified endpoint, given a list of bindings.
     *
     * @param endpoint The endpoint in which the source query is to be sent
     * @param expr The tuple expression that corresponds to the source query
     * @param bindingList a list of bindings
     * @return A publisher who publishes the evaluation results
     * @throws QueryEvaluationException
     */
    public Publisher<BindingSet> evaluate(final Site endpoint, final TupleExpr expr, final List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        optimize(expr, bindingList); // FIXME
        try {
            return evaluateReactorImpl((SPARQLSite)endpoint, expr, bindingList);
        }catch(QueryEvaluationException e)
        {
            throw e;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }

    }

    /**
     * Evaluation of a remote query to a specified endpoint, given a binding
     *
     * @param endpoint The endpoint in which the source query is to be sent
     * @param expr The tuple expression that corresponds to the source query
     * @param bindings the binding
     * @return Stream with the evaluation results
     * @throws QueryEvaluationException
     */
    public Flux<BindingSet>
        evaluateReactorImpl(final SPARQLSite endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException
    {
        Flux<BindingSet> result = null;

        try {
            Set<String> freeVars = computeVars(expr);

            freeVars.removeAll(bindings.getBindingNames());

            if (freeVars.isEmpty()) {

                // all variables in expression are bound, switch to simple ASK query

                final String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQuery(expr, freeVars);

                final BindingSet relevantBindings = bindingSetOps.project(computeVars(expr), bindings);

                result = Flux.just(bindings).flatMap(b -> {
                    try {
                        if (sendBooleanQuery(endpoint.getURL(), sparqlQuery, relevantBindings, expr))
                            return Flux.just(b);
                        else
                            return Flux.empty();
                    } catch (Exception e) {
                        return Flux.error(e);
                    }
                });

                return result;
            } else {

                final BindingSet relevantBindings = bindingSetOps.project(computeVars(expr), bindings);

                String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQuery(expr, freeVars);

                result = sendTupleQuery(endpoint.getURL(), sparqlQuery, relevantBindings, expr)
                        .map(b -> bindingSetOps.merge(bindings, b));
            }

            return result;

        } catch (QueryEvaluationException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    /**
     * Evaluation of a remote query to a specified endpoint, given a list of bindings
     *
     * @param endpoint The endpoint in which the source query is to be sent
     * @param expr The tuple expression that corresponds to the source query
     * @param bindings a list of bindings
     * @return Stream with the evaluation results
     * @throws QueryEvaluationException
     */
    protected Flux<BindingSet>
        evaluateReactorImpl(SPARQLSite endpoint, TupleExpr expr, List<BindingSet> bindings)
            throws QueryEvaluationException
    {

        if (bindings.size() == 1)
            return evaluateReactorImpl(endpoint, expr, bindings.get(0));

        Flux<BindingSet> result = null;

        Set<String> exprVars = computeVars(expr);

        Collection<String> relevant = Collections.emptySet();

        if (!bindings.isEmpty())
            relevant = BindingSetUtil.projectNames(exprVars, bindings.get(0));

        try {
            String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQueryUNION(expr, bindings, relevant);
            result = sendTupleQuery(endpoint.getURL(), sparqlQuery, EmptyBindingSet.getInstance(), expr);
            result = result.flatMap(b -> convertUnionBindings(b, bindings));
            return result;
        } catch(QueryEvaluationException e)  {
            throw e;
        } catch(Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    private Flux<BindingSet> convertUnionBindings(BindingSet rightBindings, List<BindingSet> leftBindings) {

        /*
        QueryBindingSet joinBindings = new QueryBindingSet();

        int i = -1;

        for (Binding b : rightBindings) {
            // get the relevant left binding
            String bName = b.getName();
            int splitPoint = bName.lastIndexOf("_");
            i = Integer.parseInt(bName.substring(splitPoint+1)) - 1;
            int y = i;
            // create new Binding
            joinBindings.addBinding(bName.substring(0,splitPoint),b.getValue());
        }

        for (Binding b : leftBindings.get(i)) {
            if (!joinBindings.hasBinding(b.getName()))
                joinBindings.addBinding(b);
        }

        return joinBindings;
        */

        SortedMap<Integer, QueryBindingSet> bmap = new TreeMap<Integer, QueryBindingSet>();

        int i = -1;

        for (Binding b : rightBindings) {
            // get the relevant left binding
            String bName = b.getName();
            int splitPoint = bName.lastIndexOf("_");
            i = Integer.parseInt(bName.substring(splitPoint+1)) - 1;
            Integer y = i;

            QueryBindingSet joinBindings = (bmap.containsKey(y)) ? bmap.get(y) : new QueryBindingSet();

            // create new Binding
            joinBindings.addBinding(bName.substring(0,splitPoint),b.getValue());
            bmap.put(y, joinBindings);
        }

        return Flux.fromIterable(bmap.entrySet())
                .map((join) -> bindingSetOps.merge(join.getValue(), leftBindings.get(join.getKey())));
    }

    /**
     * Sends a tuple query to a given endpoint.
     *
     * @param endpoint The endpoint in which the query is to be sent
     * @param sparqlQuery The query string to be sent
     * @param bindings A set of bindings for the query
     * @param expr The tuple expression that corresponds to the query string
     * @return Stream with the query results
     * @throws QueryEvaluationException
     * @throws MalformedQueryException
     * @throws RepositoryException
     */
    protected Flux<BindingSet>
        sendTupleQuery(URL endpoint, String sparqlQuery, BindingSet bindings, TupleExpr expr)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        LoggingUtil.logRemote(logger, conn, sparqlQuery, endpoint, expr, query);
        logger.info("Sending query {} to {} with {}", sparqlQuery, endpoint, bindings);

        return Flux.from(new TupleQueryResultPublisher(query, sparqlQuery, qfrHandler, mat, endpoint))
                .doAfterTerminate(() -> closeQuietly(conn));
    }

    /**
     * Sends a boolean query to a given endpoint.
     *
     * @param endpoint The endpoint in which the query is to be sent
     * @param sparqlQuery The query string to be sent
     * @param bindings A set of bindings for the query
     * @param expr The tuple expression that corresponds to the query string
     * @return Stream with the query results
     * @throws QueryEvaluationException
     * @throws MalformedQueryException
     * @throws RepositoryException
     */
    protected boolean
        sendBooleanQuery(URL endpoint, String sparqlQuery, BindingSet bindings, TupleExpr expr)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        BooleanQuery query = conn.prepareBooleanQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        LoggingUtil.logRemote(logger, conn, sparqlQuery, endpoint, expr, query);
        logger.info("Sending query {} to {} with {}", sparqlQuery, endpoint, bindings);

        boolean answer = query.evaluate();
        closeQuietly(conn);
        return answer;
    }


    /**
     * Compute the variable names occurring in the service expression using tree
     * traversal, since these are necessary for building the SPARQL query.
     *
     * @return the set of variable names in the given service expression
     */
    protected Set<String> computeVars(TupleExpr serviceExpression) {
        final Set<String> res = new HashSet<String>();
        serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(Var node)
                    throws RuntimeException {
                // take only real vars, i.e. ignore blank nodes
                if (!node.hasValue() && !node.isAnonymous())
                    res.add(node.getName());
            }
            // TODO maybe stop tree traversal in nested SERVICE?
            // TODO special case handling for BIND
        });
        return res;
    }
    // FIXME

    protected void optimize(TupleExpr expr, BindingSet bindings) {
        QueryOptimizer queryOptimizer =  new QueryOptimizerList(
                new BBoxDistanceOptimizer()
        );
        queryOptimizer.optimize(expr, null, bindings);
    }

    protected void optimize(TupleExpr expr, List<BindingSet> bindingSetList) {
        if (bindingSetList.isEmpty()) {
            optimize(expr, new EmptyBindingSet());
        }
        else {
            if (bindingSetList.size() == 1) {
                optimize(expr, bindingSetList.get(0));
            }
        }
    }
}
