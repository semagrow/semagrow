package eu.semagrow.cassandra.eval;

import eu.semagrow.cassandra.CassandraSite;
import eu.semagrow.cassandra.connector.CassandraClient;
import eu.semagrow.cassandra.utils.BindingSetOpsImpl;
import eu.semagrow.core.eval.QueryExecutor;
import eu.semagrow.core.eval.BindingSetOps;
import eu.semagrow.core.source.Site;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.Var;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.rx.Stream;
import reactor.rx.Streams;

import java.util.*;

/**
 * Created by antonis on 21/3/2016.
 */
public class CassandraQueryExecutorImpl implements QueryExecutor {

    /// TODO: CHECK exceptions

    private final Logger logger = LoggerFactory.getLogger(CassandraQueryExecutorImpl.class);

    protected BindingSetOps bindingSetOps = new BindingSetOpsImpl();


    @Override
    public Publisher<BindingSet> evaluate(Site site, TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {
        return evaluateCassandraImpl((CassandraSite) site, expr, bindings);
    }

    @Override
    public Publisher<BindingSet> evaluate(Site site, TupleExpr expr, List<BindingSet> bindingList)
            throws QueryEvaluationException {
        try {
            return evaluateCassandraImpl((CassandraSite) site, expr, bindingList);
        }catch(QueryEvaluationException e)
        {
            throw e;
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private Stream<BindingSet> evaluateCassandraImpl(CassandraSite site, final TupleExpr expr) {
        return sendCqlQuery(site, expr, Collections.emptyList());
    }

    private Stream<BindingSet> evaluateCassandraImpl(CassandraSite site, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException {

        if (bindings.size() == 0) {
            return evaluateCassandraImpl(site, expr);
        }

        return sendCqlQuery(site, expr, Collections.singletonList(bindings));
    }

    private Stream<BindingSet> evaluateCassandraImpl(CassandraSite site, final TupleExpr expr, List<BindingSet> bindings)
            throws QueryEvaluationException {

        if (bindings.isEmpty()) {
            return evaluateCassandraImpl(site, expr);
        }
        else {
            Set<String> freeVars = computeVars(expr);
            freeVars.removeAll(bindings.get(0).getBindingNames());
            Set<String> commonVars = computeVars(expr);
            commonVars.removeAll(freeVars);

            if (freeVars.isEmpty()) {
                return Streams.from(bindings).filter(b -> askCqlQuery(site, expr,b));
            }
            else {
                Map<BindingSet,List<BindingSet>> leftMultimap = createMultiMap(bindings, commonVars);
                Stream<BindingSet> result = sendCqlQuery(site, expr, bindings).flatMap(b -> lookup(leftMultimap, commonVars, b));
                return result;
            }
        }
    }

    private Stream<BindingSet> lookup(Map<BindingSet,List<BindingSet>> leftMultimap, Set<String> commonVars, BindingSet b) {
        BindingSet key = bindingSetOps.project(commonVars, b);
        List<BindingSet> leftList = leftMultimap.get(key);
        if (leftList == null) {
            return Streams.empty();
        }
        else {
            if (leftList.isEmpty()) {
                return Streams.empty();
            }
            else {
                return Streams.from(leftList).map(b1 -> bindingSetOps.merge(b, b1));
            }
        }
    }

    private Map<BindingSet,List<BindingSet>> createMultiMap(List<BindingSet> bindings, Set<String> commonVars) {
        Map<BindingSet,List<BindingSet>> multiMap = new HashMap<>();
        for (BindingSet b : bindings) {
            BindingSet key = bindingSetOps.project(commonVars, b);
            List<BindingSet> list = multiMap.get(key);
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(b);
            multiMap.put(key,list);
        }
        return multiMap;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private Stream<BindingSet> sendCqlQuery(CassandraSite site, TupleExpr expr, List<BindingSet> bindingsList) {

        CassandraQueryTransformer transformer = new CassandraQueryTransformer();

        String cqlQuery;

        if (bindingsList.isEmpty()) {
            cqlQuery = transformer.transformQuery(site.getBase(), expr);
        }
        else {
            cqlQuery = transformer.transformQuery(site.getBase(), expr, bindingsList);
        }

        CassandraClient client = new CassandraClient();

        client.setCredentials(site.getAddress(), site.getPort(), site.getKeyspace());
        client.connect();

        logger.info("Sending CQL query: {} to {}:{}", cqlQuery, site.getAddress(), site.getPort());

        Stream<BindingSet> result = Streams.from(client.execute(cqlQuery))
                .filter(transformer::containsAllFields)
                .map(transformer::getBindingSet);

        client.close();

        return result;
    }

    private boolean askCqlQuery(CassandraSite site, TupleExpr expr, BindingSet bindings) {
        List<BindingSet> bindingsList = new ArrayList<>();
        bindingsList.add(bindings);
        Stream<BindingSet> result = sendCqlQuery(site, expr, bindingsList);
        return (!result.equals(Streams.empty()));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    private Set<String> computeVars(TupleExpr serviceExpression) {
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
