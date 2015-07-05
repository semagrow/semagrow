package eu.semagrow.core.impl.evaluation.rx.reactor;

import eu.semagrow.core.impl.evaluation.SPARQLQueryStringUtils;
import eu.semagrow.core.impl.evaluation.rx.OnSubscribeTupleResultsReactor;
import eu.semagrow.core.impl.evaluation.rx.QueryExecutor;
import eu.semagrow.core.impl.evaluation.rx.rxjava.FederatedEvaluationStrategyImpl;
import org.openrdf.model.URI;
import org.openrdf.query.*;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
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
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.util.*;

/**
 * Created by antonis on 7/4/2015.
 */

public class QueryExecutorImpl implements QueryExecutor
{

    private final Logger logger = LoggerFactory.getLogger(eu.semagrow.core.impl.evaluation.rx.rxjava.QueryExecutorImpl.class);

    private Map<URI,Repository> repoMap = new HashMap<URI,Repository>();

    private boolean rowIdOpt = false;

    private int batchSize = 1;

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

    public void setBatchSize(int b) {
        batchSize = b;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public Publisher<BindingSet> evaluate(final URI endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorImpl(endpoint, expr, bindings);
    }

    public Publisher<BindingSet> evaluate(final URI endpoint, final TupleExpr expr, final Publisher<BindingSet> bindings)
            throws QueryEvaluationException
    {
        return evaluateReactorImpl(endpoint, expr, Streams.wrap(bindings));
    }

    public Stream<BindingSet>
        evaluateReactorImpl(final URI endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException
    {
        Stream<BindingSet> result = null;

        try {
            Set<String> freeVars = computeVars(expr);

            Set<String> relevant = getRelevantBindingNames(bindings, freeVars);
            final BindingSet relevantBindings = filterRelevant(bindings, relevant);

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
                result = Streams.just(bindings).flatMap(b -> {
                    try {
                        if (sendBooleanQueryReactor(endpoint, sparqlQuery, relevantBindings))
                            return Streams.just(b);
                        else
                            return Streams.empty();
                    } catch (Exception e) {
                        return Streams.fail(e);
                    }
                });

                return result;
            } else {
                String sparqlQuery = SPARQLQueryStringUtils.buildSPARQLQuery(expr, freeVars);
                //result = sendTupleQuery(endpoint, sparqlQuery, relevantBindings);
                //result = new InsertBindingSetCursor(result, bindings);
                result = sendTupleQueryReactor(endpoint, sparqlQuery, relevantBindings)
                        .map(b -> FederatedEvaluationStrategyImpl.joinBindings(bindings, b));
            }

            return result;

        } catch (QueryEvaluationException e) {
            throw e;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    public Stream<BindingSet>
        evaluateReactorImpl(URI endpoint, TupleExpr expr, Stream<BindingSet> bindingIter)
            throws QueryEvaluationException
    {
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

        String sparqlQuery = SPARQLQueryStringUtils.buildSPARQLQueryUNION(expr, bindings, relevant);

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


    protected BindingSet filterRelevant(BindingSet bindings, Collection<String> relevant) {
        QueryBindingSet newBindings = new QueryBindingSet();
        for (Binding b : bindings) {
            if (relevant.contains(b.getName())) {
                newBindings.setBinding(b);
            }
        }
        return newBindings;
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
}
