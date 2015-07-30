package eu.semagrow.core.impl.evaluation.rx.reactor;

import eu.semagrow.core.impl.evaluation.ConnectionManager;
import eu.semagrow.core.impl.evaluation.util.SPARQLQueryStringUtil;
import eu.semagrow.core.impl.evaluation.util.BindingSetUtil;
import eu.semagrow.core.impl.evaluation.rx.TupleQueryResultPublisher;
import eu.semagrow.core.impl.evaluation.rx.QueryExecutor;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.reactivestreams.Publisher;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.util.*;

/**
 * Created by antonis on 7/4/2015.
 */

public class QueryExecutorImpl extends ConnectionManager implements QueryExecutor
{
    private boolean rowIdOpt = false;

    public Publisher<BindingSet> evaluate(final URI endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorImpl(endpoint, expr, bindings);
    }

    public Publisher<BindingSet> evaluate(final URI endpoint, final TupleExpr expr, final List<BindingSet> bindingList)
            throws QueryEvaluationException
    {
        try {
            return evaluateReactorImpl(endpoint, expr, bindingList);
        }catch(QueryEvaluationException e)
        {
            throw e;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }

    }

    public Stream<BindingSet>
        evaluateReactorImpl(final URI endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException
    {
        Stream<BindingSet> result = null;

        try {
            Set<String> freeVars = computeVars(expr);

            freeVars.removeAll(bindings.getBindingNames());

            if (freeVars.isEmpty()) {

                // all variables in expression are bound, switch to simple ASK query

                final String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQuery(expr, freeVars);

                final BindingSet relevantBindings = BindingSetUtil.project(computeVars(expr), bindings);

                result = Streams.just(bindings).flatMap(b -> {
                    try {
                        if (sendBooleanQuery(endpoint, sparqlQuery, relevantBindings))
                            return Streams.just(b);
                        else
                            return Streams.empty();
                    } catch (Exception e) {
                        return Streams.fail(e);
                    }
                });

                return result;
            } else {

                final BindingSet relevantBindings = BindingSetUtil.project(computeVars(expr), bindings);

                String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQuery(expr, freeVars);

                result = sendTupleQuery(endpoint, sparqlQuery, relevantBindings)
                        .map(b -> BindingSetUtil.merge(bindings, b));
            }

            return result;

        } catch (QueryEvaluationException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    protected Stream<BindingSet>
        evaluateReactorImpl(URI endpoint, TupleExpr expr, List<BindingSet> bindings)
            throws QueryEvaluationException
    {

        if (bindings.size() == 1)
            return evaluateReactorImpl(endpoint, expr, bindings.get(0));

        Stream<BindingSet> result = null;

        Set<String> exprVars = computeVars(expr);

        Collection<String> relevant = Collections.emptySet();

        if (!bindings.isEmpty())
            relevant = BindingSetUtil.projectNames(exprVars, bindings.get(0));

        try {
            String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQueryUNION(expr, bindings, relevant);
            result = sendTupleQuery(endpoint, sparqlQuery, EmptyBindingSet.getInstance());
            result = result.concatMap(b -> convertUnionBindings(b, bindings));
            return result;
        } catch(QueryEvaluationException e)  {
            throw e;
        } catch(Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    private Stream<BindingSet> convertUnionBindings(BindingSet rightBindings, List<BindingSet> leftBindings) {

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

        return Streams.from(bmap.entrySet())
                .map((join) -> BindingSetUtil.merge(join.getValue(), leftBindings.get(join.getKey())));
    }

    protected Stream<BindingSet>
        sendTupleQuery(URI endpoint, String sparqlQuery, BindingSet bindings)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());


        return Streams.wrap(new TupleQueryResultPublisher(query))
                .finallyDo((s) -> closeQuietly(conn));
    }

    protected boolean
        sendBooleanQuery(URI endpoint, String sparqlQuery, BindingSet bindings)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        BooleanQuery query = conn.prepareBooleanQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        logger.debug("Sending to {} query {} with {}", endpoint.stringValue(), sparqlQuery.replace('\n', ' '), query.getBindings());

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
        serviceExpression.visit(new QueryModelVisitorBase<RuntimeException>() {

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
}
