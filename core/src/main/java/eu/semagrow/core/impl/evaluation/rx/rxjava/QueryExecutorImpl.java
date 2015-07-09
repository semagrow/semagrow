package eu.semagrow.core.impl.evaluation.rx.rxjava;

import eu.semagrow.core.impl.evaluation.util.SPARQLQueryStringUtils;
import eu.semagrow.core.impl.evaluation.util.BindingSetUtils;
import eu.semagrow.core.impl.evaluation.rx.QueryExecutor;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import rx.Observable;
import rx.functions.Func2;
import rx.RxReactiveStreams;

/**
 * Created by angel on 11/25/14.
 */
public class QueryExecutorImpl implements QueryExecutor
{

    private final Logger logger = LoggerFactory.getLogger(QueryExecutorImpl.class);

    private Map<URI,Repository> repoMap = new HashMap<URI,Repository>();

    private boolean rowIdOpt = false;

    private int countconn = 0;

    public RepositoryConnection getConnection(URI endpoint) throws RepositoryException {
        Repository repo = null;

        if (!repoMap.containsKey(endpoint)) {
            repo = new SPARQLRepository(endpoint.stringValue());
            repoMap.put(endpoint,repo);
        } else {
            repo = repoMap.get(endpoint);
        }

        if (!repo.isInitialized())
            repo.initialize();

        RepositoryConnection conn = repo.getConnection();
        logger.debug("Connection " + conn.toString() +" started, currently open " + countconn);
        countconn++;
        return conn;
    }

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

            final BindingSet relevantBindings = BindingSetUtils.project(freeVars, bindings);

            freeVars.removeAll(bindings.getBindingNames());

            if (freeVars.isEmpty()) {

                final String sparqlQuery = SPARQLQueryStringUtils.buildSPARQLQuery(expr, freeVars);

                /*
                result = new DelayedIteration<BindingSet, QueryEvaluationException>() {
                    @Override
                    protected Iteration<? extends BindingSet, ? extends QueryEvaluationException> createIteration()
                            throws QueryEvaluationException {
                        try {
                            boolean askAnswer = sendBooleanQuery(endpoint, sparqlQuery, relevantBindings);
                            if (askAnswer) {
                                return new SingletonIteration<BindingSet, QueryEvaluationException>(bindings);
                            } else {
                                return new EmptyIteration<BindingSet, QueryEvaluationException>();
                            }
                        } catch (QueryEvaluationException e) {
                            throw e;
                        } catch (Exception e) {
                            throw new QueryEvaluationException(e);
                        }
                    }
                };
                */
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
                String sparqlQuery = SPARQLQueryStringUtils.buildSPARQLQuery(expr, freeVars);
                //result = sendTupleQuery(endpoint, sparqlQuery, relevantBindings);
                //result = new InsertBindingSetCursor(result, bindings);
                result = sendTupleQueryReactive(endpoint, sparqlQuery, relevantBindings)
                    .map(b -> BindingSetUtils.merge(bindings, b));
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

            /*
            return bindingIter.flatMap(b -> {
                try {
                    return evaluateReactive(endpoint, expr, b);
                } catch (QueryEvaluationException e2) {
                    return Observable.error(e2);
                }
            });
            */

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

        String sparqlQuery = SPARQLQueryStringUtils.buildSPARQLQueryUNION(expr, bindings, relevant);

        result = sendTupleQueryReactive(endpoint, sparqlQuery, EmptyBindingSet.getInstance());

        result = result.map(b -> convertUnionBindings(b, bindings, BindingSetUtils::merge));

        /*if (!relevant.isEmpty()) {

            final Observable<BindingSet> r = result;

            result = Observable.from(bindings)
                    .toMultimap(b -> FederatedReactiveEvaluationStrategyImpl.calcKey(b, relevant), b1 -> b1)
                    .flatMap(probe ->
                            r.concatMap(b -> {
                                BindingSet k = FederatedReactiveEvaluationStrategyImpl.calcKey(b, relevant);
                                if (!probe.containsKey(k))
                                    return Observable.empty();
                                else
                                    return Observable.from(probe.get(k))
                                            .merge(Observable.just(b),
                                                    b1 -> Observable.never(),
                                                    b1 -> Observable.never(),
                                                     BindingSetUtils::merge);
                            }));

        }
        else {

            result = result.merge(Observable.from(bindings),
                        (b) -> Observable.never(),
                        (b) -> Observable.never(),
                         BindingSetUtils::merge);
        }*/

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
                .doOnCompleted(() -> {
                    try {
                        if (conn.isOpen()) {
                            conn.close();
                            logger.debug("Connection " + conn.toString() + " closed");
                        }
                    } catch (RepositoryException e) {
                        logger.debug("Connection cannot be closed", e);
                    }
                });
    }

    protected boolean
        sendBooleanQueryReactive(URI endpoint, String sparqlQuery, BindingSet bindings)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        BooleanQuery query = conn.prepareBooleanQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        logger.debug("Sending to " + endpoint.stringValue() + " query " + sparqlQuery.replace('\n', ' ') + " with " + query.getBindings());
        return query.evaluate();
    }

}
