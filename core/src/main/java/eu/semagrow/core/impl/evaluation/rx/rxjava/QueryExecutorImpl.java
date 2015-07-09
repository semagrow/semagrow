package eu.semagrow.core.impl.evaluation.rx.rxjava;

import eu.semagrow.core.impl.evaluation.ConnectionManager;
import eu.semagrow.core.impl.evaluation.util.SPARQLQueryStringUtil;
import eu.semagrow.core.impl.evaluation.util.BindingSetUtil;
import eu.semagrow.core.impl.evaluation.rx.QueryExecutor;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.reactivestreams.Publisher;

import java.util.*;
import rx.Observable;
import rx.functions.Func2;
import rx.RxReactiveStreams;

/**
 * Created by angel on 11/25/14.
 */
public class QueryExecutorImpl extends ConnectionManager implements QueryExecutor
{
    private boolean rowIdOpt = false;

    public Publisher<BindingSet> evaluate(final URI endpoint, final TupleExpr expr, final BindingSet bindings)
        throws QueryEvaluationException
    {
        return RxReactiveStreams.toPublisher(evaluateReactiveImpl(endpoint, expr, bindings));
    }

    public Publisher<BindingSet> evaluate(final URI endpoint, final TupleExpr expr, final List<BindingSet> bindings)
            throws QueryEvaluationException
    {
        //Observable<BindingSet> observableBindings = RxReactiveStreams.toObservable(bindings);
        return RxReactiveStreams.toPublisher(evaluateReactiveImpl(endpoint, expr, bindings));
    }


    public Observable<BindingSet>
        evaluateReactiveImpl(final URI endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException {

        Observable<BindingSet> result = null;
        try {

            Set<String> freeVars = computeVars(expr);

            //Collection<String> relevant = BindingSetUtils.projectNames(freeVars, bindings);

            final BindingSet relevantBindings = BindingSetUtil.project(freeVars, bindings);

            freeVars.removeAll(bindings.getBindingNames());

            if (freeVars.isEmpty()) {

                final String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQuery(expr, freeVars);

                result = Observable.just(bindings).flatMap(b -> {
                    try {
                        if (sendBooleanQueryReactive(endpoint, sparqlQuery, relevantBindings))
                            return Observable.just(b);
                        else
                            return Observable.empty();
                    } catch (Exception e) {
                        return Observable.error(e);
                    }
                });

                return result;
            } else {
                String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQuery(expr, freeVars);
                //result = sendTupleQuery(endpoint, sparqlQuery, relevantBindings);
                //result = new InsertBindingSetCursor(result, bindings);
                result = sendTupleQueryReactive(endpoint, sparqlQuery, relevantBindings)
                    .map(b -> BindingSetUtil.merge(bindings, b));
            }

            return result;

        } catch (QueryEvaluationException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    public Observable<BindingSet>
        evaluateReactiveImpl(URI endpoint, TupleExpr expr,
             List<BindingSet> bl)
            throws QueryEvaluationException {

        //Observable<BindingSet> result = null;

        try {

            return  evaluateReactiveInternal(endpoint, expr, bl);

        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }


    protected Observable<BindingSet>
        evaluateReactiveInternal(URI endpoint, TupleExpr expr, List<BindingSet> bindings)
            throws Exception
    {

        if (bindings.size() == 1)
            return evaluateReactiveImpl(endpoint, expr, bindings.get(0));

        Observable<BindingSet> result = null;

        Set<String> exprVars = computeVars(expr);

        Set<String> relevant = new HashSet<String>(getRelevantBindingNames(bindings, exprVars));

        String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQueryUNION(expr, bindings, relevant);

        result = sendTupleQueryReactive(endpoint, sparqlQuery, EmptyBindingSet.getInstance());

        result = result.map(b -> convertUnionBindings(b, bindings, BindingSetUtil::merge));

        return result;
    }


    protected Set<String> getRelevantBindingNames(List<BindingSet> bindings, Set<String> exprVars) {

        if (bindings.isEmpty())
            return Collections.emptySet();

        return getRelevantBindingNames(bindings.get(0), exprVars);
    }

    protected Set<String> getRelevantBindingNames(BindingSet bindings, Set<String> exprVars){
        Set<String> relevantBindingNames = new HashSet<String>(5);
        for (String bName : bindings.getBindingNames()) {
            if (exprVars.contains(bName))
                relevantBindingNames.add(bName);
        }

        return relevantBindingNames;
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

    private BindingSet convertUnionBindings(BindingSet rightBindings,
                                            List<BindingSet> leftBindings,
                                            Func2<? extends BindingSet, ? extends BindingSet, BindingSet> f) {

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

    }

    protected Observable<BindingSet>
        sendTupleQueryReactive(URI endpoint, String sparqlQuery, BindingSet bindings)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        return Observable.create(new OnSubscribeTupleResults(query)).onBackpressureBuffer()
                .doOnCompleted(() -> closeQuietly(conn));
    }

    protected boolean
        sendBooleanQueryReactive(URI endpoint, String sparqlQuery, BindingSet bindings)
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

}
