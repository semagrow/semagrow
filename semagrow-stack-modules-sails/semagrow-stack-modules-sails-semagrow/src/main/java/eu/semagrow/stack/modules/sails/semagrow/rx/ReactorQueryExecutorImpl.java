package eu.semagrow.stack.modules.sails.semagrow.rx;

import eu.semagrow.stack.modules.sails.semagrow.evaluation.QueryExecutorImpl;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.util.*;

/**
 * Created by antonis on 7/4/2015.
 */

public class ReactorQueryExecutorImpl
        extends QueryExecutorImpl
        implements ReactiveQueryExecutor
{

    private final Logger logger = LoggerFactory.getLogger(ReactiveQueryExecutorImpl.class);

    private Map<URI,Repository> repoMap = new HashMap<URI,Repository>();

    private boolean rowIdOpt = false;

    private int batchSize = 1;

    public void setBatchSize(int b) {
        batchSize = b;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Publisher<BindingSet> evaluateReactive(final URI endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorImpl(endpoint, expr, bindings);
    }

    public Publisher<BindingSet> evaluateReactive(final URI endpoint, final TupleExpr expr, final Publisher<BindingSet> bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorImpl(endpoint, expr, Streams.wrap(bindings));
    }

    public Stream<BindingSet>
    evaluateReactorImpl(final URI endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException {

        Stream<BindingSet> result = null;
        try {
            Set<String> freeVars = computeVars(expr);

            Set<String> relevant = getRelevantBindingNames(bindings, freeVars);
            final BindingSet relevantBindings = filterRelevant(bindings, relevant);

            freeVars.removeAll(bindings.getBindingNames());

            if (freeVars.isEmpty()) {

                final String sparqlQuery = buildSPARQLQuery(expr, freeVars);

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
                String sparqlQuery = buildSPARQLQuery(expr, freeVars);
                //result = sendTupleQuery(endpoint, sparqlQuery, relevantBindings);
                //result = new InsertBindingSetCursor(result, bindings);
                result = sendTupleQueryReactor(endpoint, sparqlQuery, relevantBindings)
                        .map(b -> FederatedReactiveEvaluationStrategyImpl.joinBindings(bindings, b));
            }

            return result;

        } catch (QueryEvaluationException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    public Stream<BindingSet>
    evaluateReactorImpl(URI endpoint, TupleExpr expr,
                         Stream<BindingSet> bindingIter)
            throws QueryEvaluationException {

        //Stream<BindingSet> result = null;

        try {


            return bindingIter.buffer(batchSize).concatMap(
                    bl ->  { try {
                        return evaluateReactorInternal(endpoint, expr, bl);
                    } catch (Exception e) {
                        return Streams.fail(e);
                    } });


            /*
            return bindingIter.flatMap(b -> {
                try {
                    return evaluateReactive(endpoint, expr, b);
                } catch (QueryEvaluationException e2) {
                    return Stream.error(e2);
                }
            });
            */

        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }


    protected Stream<BindingSet>
    evaluateReactorInternal(URI endpoint, TupleExpr expr, List<BindingSet> bindings)
            throws Exception
    {

        if (bindings.size() == 1)
            return evaluateReactorImpl(endpoint, expr, bindings.get(0));

        Stream<BindingSet> result = null;

        Set<String> exprVars = computeVars(expr);

        Set<String> relevant = new HashSet<String>(getRelevantBindingNames(bindings, exprVars));

        String sparqlQuery = buildSPARQLQueryUNION(expr, bindings, relevant);

        result = sendTupleQueryReactor(endpoint, sparqlQuery, EmptyBindingSet.getInstance());

        result = result.map(b -> convertUnionBindings(b, bindings));

        /*if (!relevant.isEmpty()) {

            final Stream<BindingSet> r = result;

            HashMap<BindingSet, List<BindingSet>> probe = new HashMap();
            for (BindingSet b : bindings) {
                List bs = probe.get(FederatedReactiveEvaluationStrategyImpl.calcKey(b, relevant));
                if (bs == null)
                    bs = new ArrayList<BindingSet>();
                bs.add(b);
                probe.put(FederatedReactiveEvaluationStrategyImpl.calcKey(b, relevant), bs);
            }

            result = r.concatMap(b -> {
                        BindingSet k = FederatedReactiveEvaluationStrategyImpl.calcKey(b, relevant);
                        List<BindingSet> bb = probe.get(k);
                        if (!probe.containsKey(k))
                            return Streams.empty();
                        else
                            return Streams.from(bb).map(bbb -> FederatedReactiveEvaluationStrategyImpl.joinBindings(b, bbb));
                    }
            );

        }
       else {
            final Stream<BindingSet> r = result;

            result = r.concatMap(b ->
                     Streams.from(bindings).map(bbb -> FederatedReactiveEvaluationStrategyImpl.joinBindings(b, bbb))
            );
        }*/

        return result;
    }

    private BindingSet convertUnionBindings(BindingSet rightBindings, List<BindingSet> leftBindings) {

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

    protected Stream<BindingSet>
    sendTupleQueryReactor(URI endpoint, String sparqlQuery, BindingSet bindings)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        return Streams.wrap(new OnSubscribeTupleResultsReactor(query)) /* onBackpressureBuffer()
                .doOnCompleted(() -> {
                    try {
                        if (conn.isOpen()) {
                            conn.close();
                            logger.debug("Connection " + conn.toString() + " closed");
                        }
                    } catch (RepositoryException e) {
                        logger.debug("Connection cannot be closed", e);
                    }
                }) */;
        //return Streams.empty();
    }

    protected boolean
    sendBooleanQueryReactor(URI endpoint, String sparqlQuery, BindingSet bindings)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        BooleanQuery query = conn.prepareBooleanQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        logger.debug("Sending to " + endpoint.stringValue() + " query " + sparqlQuery.replace('\n', ' ') + " with " + query.getBindings());
        return query.evaluate();
    }
}
