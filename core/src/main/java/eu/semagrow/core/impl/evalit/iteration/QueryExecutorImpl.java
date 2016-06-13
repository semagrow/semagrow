package eu.semagrow.core.impl.evalit.iteration;

import eu.semagrow.core.evalit.QueryExecutor;
import eu.semagrow.core.impl.sparql.ConnectionManager;
import eu.semagrow.core.impl.sparql.SPARQLQueryStringUtil;
import org.eclipse.rdf4j.common.iteration.*;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryBindingSet;
import org.eclipse.rdf4j.query.algebra.evaluation.federation.JoinExecutorBase;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.CollectionIteration;
import org.eclipse.rdf4j.query.algebra.evaluation.iterator.CrossProductIteration;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.impl.EmptyBindingSet;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sparql.query.InsertBindingSetCursor;

import java.util.*;

/**
 * Created by angel on 6/6/14.
 */
//FIXME: Shutdown connections and repositories properly
public class QueryExecutorImpl extends ConnectionManager implements QueryExecutor {

    private boolean rowIdOpt = false;


    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(final IRI endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException {

        CloseableIteration<BindingSet,QueryEvaluationException> result = null;
        try {
            Set<String> freeVars = computeVars(expr);

            Set<String> relevant = getRelevantBindingNames(bindings, freeVars);
            final BindingSet relevantBindings = filterRelevant(bindings, relevant);

            freeVars.removeAll(bindings.getBindingNames());

            if (freeVars.isEmpty()) {

                final String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQuery(expr, freeVars);

                result = askToIteration(endpoint, sparqlQuery, bindings, relevantBindings);
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
            } else {
                String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQuery(expr, freeVars);
                result = sendTupleQuery(endpoint, sparqlQuery, relevantBindings);
                result = new InsertBindingSetCursor(result, bindings);
            }

            return result;

        } catch (QueryEvaluationException e) {
            Iterations.closeCloseable(result);
            throw e;
        } catch (Exception e) {
            Iterations.closeCloseable(result);
            throw new QueryEvaluationException(e);
        }
    }

    private CloseableIteration<BindingSet, QueryEvaluationException>
        askToIteration(IRI endpoint, String sparqlQuery, BindingSet bindings, BindingSet relevantBindings)
        throws QueryEvaluationException
    {
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

    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(IRI endpoint, TupleExpr expr,
                 CloseableIteration<BindingSet, QueryEvaluationException> bindingIter)
            throws QueryEvaluationException {

        CloseableIteration<BindingSet, QueryEvaluationException> result = null;
        try {
            List<BindingSet> bindings = Iterations.asList(bindingIter);

            if (bindings.isEmpty()) {
                return new EmptyIteration<BindingSet, QueryEvaluationException>();
            }

            if (bindings.size() == 1) {
                result = evaluate(endpoint, expr, bindings.get(0));
                return result;
            }


            try {
                result = evaluateInternal(endpoint, expr, bindings);
                return result;
            } catch(Exception e) {
                logger.debug("Failover to sequential iteration", e);
                return new SequentialQueryIteration(endpoint, expr, bindings);
            }

            //return new SequentialQueryIteration(endpoint, expr, bindings);

        } /*catch (MalformedQueryException e) {
                // this exception must not be silenced, bug in our code
                throw new QueryEvaluationException(e);
        }*/
        catch (QueryEvaluationException e) {
            if (result != null)
                Iterations.closeCloseable(result);
            throw e;
        } catch (Exception e) {
            if (result != null)
                Iterations.closeCloseable(result);
            throw new QueryEvaluationException(e);
        }
    }


    protected CloseableIteration<BindingSet, QueryEvaluationException>
        evaluateInternal(IRI endpoint, TupleExpr expr, List<BindingSet> bindings)
            throws Exception {

        CloseableIteration<BindingSet, QueryEvaluationException> result = null;

        Set<String> exprVars = computeVars(expr);

        Set<String> relevant = getRelevantBindingNames(bindings, exprVars);

        //String sparqlQuery = buildSPARQLQueryVALUES(expr, bindings, relevant);
        String sparqlQuery = SPARQLQueryStringUtil.buildSPARQLQueryUNION(expr, bindings, relevant);

        result = sendTupleQuery(endpoint, sparqlQuery, EmptyBindingSet.getInstance());

        if (!relevant.isEmpty()) {
            if (rowIdOpt)
                result = new InsertValuesBindingsIteration(result, bindings);
            else {
                result = new UnionJoinIteration(
                                new CollectionIteration<BindingSet, QueryEvaluationException>(bindings),
                                result,
                                new HashSet<String>(relevant));
            }

        }
        else {

        	result = new CrossProductIteration(result, bindings);

        }
        return result;
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
        serviceExpression.visit(new AbstractQueryModelVisitor<RuntimeException>() {

            @Override
            public void meet(Var node)
                    throws RuntimeException
            {
                // take only real vars, i.e. ignore blank nodes
                if (!node.hasValue() && !node.isAnonymous())
                    res.add(node.getName());
            }
            // TODO maybe stop tree traversal in nested SERVICE?
            // TODO special case handling for BIND
        });
        return res;
    }

    protected CloseableIteration<BindingSet, QueryEvaluationException>
        sendTupleQuery(IRI endpoint, String sparqlQuery, BindingSet bindings)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        logger.debug("Sending to " + endpoint.stringValue() + " query " + sparqlQuery.replace('\n', ' '));
        return closeConnAfter(conn, query.evaluate());
    }

    private  <E,X extends Exception> CloseableIteration<E,X>
        closeConnAfter(RepositoryConnection conn, CloseableIteration<E,X> iter) {
        return new CloseConnAfterIteration<E,X>(conn,iter);
    }

    protected boolean
        sendBooleanQuery(IRI endpoint, String sparqlQuery, BindingSet bindings)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        BooleanQuery query = conn.prepareBooleanQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        logger.debug("Sending to " + endpoint.stringValue() + " query " + sparqlQuery.replace('\n', ' '));
        boolean answer = query.evaluate();
        conn.close();
        return answer;
    }

    protected class SequentialQueryIteration extends JoinExecutorBase<BindingSet> {

        private TupleExpr expr;
        private IRI endpoint;
        private Collection<BindingSet> bindings;

        public SequentialQueryIteration(IRI endpoint, TupleExpr expr, Collection<BindingSet> bindings)
                throws QueryEvaluationException {
            super(null, null, EmptyBindingSet.getInstance());
            this.endpoint = endpoint;
            this.expr = expr;
            this.bindings = bindings;
            run();
        }

        @Override
        protected void handleBindings() throws QueryEvaluationException {
            for (BindingSet b : bindings) {
                CloseableIteration<BindingSet,QueryEvaluationException> result = evaluate(endpoint, expr, b);
                addResult(result);
            }
        }
    }

    private class CloseConnAfterIteration<E,X extends Exception> extends IterationWrapper<E,X> {

        private RepositoryConnection conn;

        public CloseConnAfterIteration(RepositoryConnection conn, Iteration<? extends E, ? extends X> iter) {
            super(iter);
            assert conn != null;
            this.conn = conn;
        }

        @Override
        public void handleClose() throws X {
            super.handleClose();

            closeQuietly(conn);
        }
    }


}
