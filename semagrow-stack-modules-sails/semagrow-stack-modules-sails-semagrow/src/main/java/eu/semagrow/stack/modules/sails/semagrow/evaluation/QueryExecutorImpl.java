package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import eu.semagrow.stack.modules.api.evaluation.QueryExecutor;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.HashJoinIteration;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.InsertValuesBindingsIteration;
import eu.semagrow.stack.modules.sails.semagrow.evaluation.iteration.UnionJoinIteration;
import info.aduna.iteration.*;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.*;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.openrdf.query.algebra.evaluation.federation.JoinExecutorBase;
import org.openrdf.query.algebra.evaluation.federation.ServiceCrossProductIteration;
import org.openrdf.query.algebra.evaluation.iterator.CollectionIteration;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.impl.EmptyBindingSet;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sparql.SPARQLRepository;
import org.openrdf.repository.sparql.query.InsertBindingSetCursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.regex.Matcher;

/**
 * Created by angel on 6/6/14.
 */
//FIXME: Shutdown connections and repositories properly
public class QueryExecutorImpl implements QueryExecutor {

    private final Logger logger = LoggerFactory.getLogger(QueryExecutorImpl.class);

    private int countconn = 0;

    private Map<URI,Repository> repoMap = new HashMap<URI,Repository>();

    private boolean rowIdOpt = false;

    public void initialize() { }

    public void shutdown() { }

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

    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(final URI endpoint, final TupleExpr expr, final BindingSet bindings)
            throws QueryEvaluationException {

        CloseableIteration<BindingSet,QueryEvaluationException> result = null;
        try {
            Set<String> freeVars = computeVars(expr);

            Set<String> relevant = getRelevantBindingNames(bindings, freeVars);
            final BindingSet relevantBindings = filterRelevant(bindings, relevant);

            freeVars.removeAll(bindings.getBindingNames());

            if (freeVars.isEmpty()) {

                final String sparqlQuery = buildSPARQLQuery(expr, freeVars);

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
                String sparqlQuery = buildSPARQLQuery(expr, freeVars);
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
        askToIteration(URI endpoint, String sparqlQuery, BindingSet bindings, BindingSet relevantBindings)
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
        evaluate(URI endpoint, TupleExpr expr,
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
        evaluateInternal(URI endpoint, TupleExpr expr, List<BindingSet> bindings)
            throws Exception {

        CloseableIteration<BindingSet, QueryEvaluationException> result = null;

        Set<String> exprVars = computeVars(expr);

        Set<String> relevant = getRelevantBindingNames(bindings, exprVars);

        //String sparqlQuery = buildSPARQLQueryVALUES(expr, bindings, relevant);
        String sparqlQuery = buildSPARQLQueryUNION(expr, bindings, relevant);

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

        	result = new ServiceCrossProductIteration(result, bindings);

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
        serviceExpression.visit(new QueryModelVisitorBase<RuntimeException>() {

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
        sendTupleQuery(URI endpoint, String sparqlQuery, BindingSet bindings)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        logger.debug("Sending to " + endpoint.stringValue() + " query " + sparqlQuery.replace('\n', ' '));
        return closeConnAfter(conn, query.evaluate());
    }

    private static <E,X extends Exception> CloseableIteration<E,X> closeConnAfter(RepositoryConnection conn, CloseableIteration<E,X> iter) {
        return new CloseConnAfterIteration<E,X>(conn,iter);
    }

    protected boolean
        sendBooleanQuery(URI endpoint, String sparqlQuery, BindingSet bindings)
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

    /**
     * Computes the VALUES clause for the set of relevant input bindings. The
     * VALUES clause is attached to a subquery for block-nested-loop evaluation.
     * Implementation note: we use a special binding to mark the rowIndex of the
     * input binding.
     *
     * @param bindings
     * @param relevantBindingNames
     * @return a string with the VALUES clause for the given set of relevant
     *         input bindings
     * @throws QueryEvaluationException
     */
    private String buildVALUESClause(List<BindingSet> bindings, Set<String> relevantBindingNames)
            throws QueryEvaluationException
    {

        if (relevantBindingNames.isEmpty())
            return "";

        StringBuilder sb = new StringBuilder();
        sb.append(" VALUES (");

        if (rowIdOpt)
            sb.append(" ?"+ InsertValuesBindingsIteration.INDEX_BINDING_NAME);

        for (String bName : relevantBindingNames) {
            sb.append(" ?").append(bName);
        }

        sb.append(") { ");

        int rowIdx = 0;
        for (BindingSet b : bindings) {
            sb.append(" (");

            if (rowIdOpt) {
                sb.append("\"").append(rowIdx++).append("\" "); // identification of the row for post processing
            }

            for (String bName : relevantBindingNames) {
                appendValueAsString(sb, b.getValue(bName)).append(" ");
            }
            sb.append(")");
        }

        sb.append(" }");
        return sb.toString();
    }

    protected String buildSPARQLQuery(TupleExpr expr, Collection<String> projection) throws Exception {
        if (projection != null && projection.isEmpty())
            return buildAskSPARQLQuery(expr);
        else
            return buildSelectSPARQLQuery(expr, projection);
    }

    protected String buildSelectSPARQLQuery(TupleExpr expr, Collection<String> projection)
            throws Exception {


        TupleExpr body = expr.clone();

        if (projection != null) {
            Projection proj = new Projection();
            ProjectionElemList elemList = new ProjectionElemList();

            for (String var : projection)
                elemList.addElement(new ProjectionElem(var));

            proj.setProjectionElemList(elemList);

            proj.setArg(body);
            body = proj;
        }

        ParsedTupleQuery query = new ParsedTupleQuery(body);

        return new SPARQLQueryRenderer().render(query);
    }

    private String buildAskSPARQLQuery(TupleExpr expr)
            throws Exception {
        ParsedBooleanQuery query = new ParsedBooleanQuery(expr);
        return new SPARQLQueryRenderer().render(query);
    }

    protected String buildSPARQLQueryVALUES(TupleExpr expr, List<BindingSet> bindings, Set<String> relevantBindingNames)
            throws Exception {

        Set<String> freeVars = computeVars(expr);

        if (rowIdOpt)
            freeVars.add(InsertValuesBindingsIteration.INDEX_BINDING_NAME);

        //freeVars.removeAll(relevantBindingNames);

        return buildSPARQLQuery(expr,freeVars) + buildVALUESClause(bindings,relevantBindingNames);
    }

    ////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////

    protected String buildSPARQLQueryUNION(TupleExpr expr, List<BindingSet> bindings, Set<String> relevantBindingNames)
            throws Exception {

        Set<String> freeVars = computeVars(expr);
        freeVars.removeAll(relevantBindingNames);
        if (freeVars.isEmpty()) {
            return buildSPARQLQueryUNIONFILTER(expr, bindings, relevantBindingNames);
        }
        String query = new SPARQLQueryRenderer().render(new ParsedTupleQuery(expr));
        String where = query.substring(query.indexOf('{'));
        StringBuilder sb = new StringBuilder();
        int i = 1;
        boolean flag = false;
        for (BindingSet b : bindings) {
            if (flag) {
                sb.append(" UNION ");
            }
            flag = true;
            String tmpStr = where;
            for (String name : relevantBindingNames) {
                String pattern = "[\\?\\$]" + name + "(?=\\W)";
                StringBuilder val = new StringBuilder();
                appendValueAsString(val, b.getValue(name));
                String replacement = val.toString();
                tmpStr = tmpStr.replaceAll(pattern, Matcher.quoteReplacement(replacement));
            }
            for (String name : freeVars) {
                String pattern = "[\\?\\$]" + name + "(?=\\W)";
                String replacement = "?" + name + "_" + i;
                tmpStr = tmpStr.replaceAll(pattern, Matcher.quoteReplacement(replacement));
            }
            sb.append(tmpStr);
            i++;
        }
        sb.append(" }");
        String pr = "SELECT ";
        for (int j=1; j<i; j++) {
            for (String name : freeVars) {
                pr = pr + "?" + name + "_" + j + " ";
            }
        }
        pr = pr + "\nWHERE { ";
        return (pr + sb.toString());
    }
    private String buildSPARQLQueryUNIONFILTER(TupleExpr expr, List<BindingSet> bindings, Set<String> relevantBindingNames)
            throws Exception {
        String query = new SPARQLQueryRenderer().render(new ParsedTupleQuery(expr));
        String where = query.substring(query.indexOf('{'));
        StringBuilder sb = new StringBuilder();
        int i = 1;
        for (BindingSet b : bindings) {
            String tmpStr = where;
            for (String name : relevantBindingNames) {
                String pattern = "[\\?\\$]" + name + "(?=\\W)";
                String replacement = "?" + name + "_" + i;
                tmpStr = tmpStr.replaceAll(pattern, Matcher.quoteReplacement(replacement));
            }
            tmpStr = tmpStr.substring(1,tmpStr.lastIndexOf('}'));
            sb.append(tmpStr);
            sb.append(" FILTER (");
            boolean flag = false;
            for (String name : relevantBindingNames) {
                if (flag) {
                    sb.append(" and ");
                }
                flag = true;
                sb.append("?"+name + "_" + i +"=");
                appendValueAsString(sb, b.getValue(name));
            }
            sb.append(") } UNION {");
            i++;
        }
        sb.append("} }");
        String pr = "SELECT ";
        for (int j=1; j<i; j++) {
            for (String name : relevantBindingNames) {
                pr = pr + "?" + name + "_" + j + " ";
            }
        }
        pr = pr + "\nWHERE { { ";
        return (pr + sb.toString());
    }
    ////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////

    protected StringBuilder appendValueAsString(StringBuilder sb, Value value) {

        // TODO check if there is some convenient method in Sesame!

        if (value == null)
            return sb.append("UNDEF"); // see grammar for BINDINGs def

        else if (value instanceof URI)
            return appendURI(sb, (URI)value);

        else if (value instanceof Literal)
            return appendLiteral(sb, (Literal)value);

        // XXX check for other types ? BNode ?
        throw new RuntimeException("Type not supported: " + value.getClass().getCanonicalName());
    }

    /**
     * Append the uri to the stringbuilder, i.e. <uri.stringValue>.
     *
     * @param sb
     * @param uri
     * @return the StringBuilder, for convenience
     */
    protected static StringBuilder appendURI(StringBuilder sb, URI uri) {
        sb.append("<").append(uri.stringValue()).append(">");
        return sb;
    }

    /**
     * Append the literal to the stringbuilder: "myLiteral"^^<dataType>
     *
     * @param sb
     * @param lit
     * @return the StringBuilder, for convenience
     */
    protected static StringBuilder appendLiteral(StringBuilder sb, Literal lit) {
        sb.append('"');
        sb.append(lit.getLabel().replace("\"", "\\\""));
        sb.append('"');

        //if (Literals.isLanguageLiteral(lit)) {
        //    sb.append('@');
        //    sb.append(lit.getLanguage());
       // }
        //else {
        if (lit.getDatatype() != null) {
            sb.append("^^<");
            sb.append(lit.getDatatype().stringValue());
            sb.append('>');
        }
        //}
        return sb;
    }

    protected class SequentialQueryIteration extends JoinExecutorBase<BindingSet> {

        private TupleExpr expr;
        private URI endpoint;
        private Collection<BindingSet> bindings;

        public SequentialQueryIteration(URI endpoint, TupleExpr expr, Collection<BindingSet> bindings)
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



    private static class CloseConnAfterIteration<E,X extends Exception> extends IterationWrapper<E,X> {

        private RepositoryConnection conn;

        public CloseConnAfterIteration(RepositoryConnection conn, Iteration<? extends E, ? extends X> iter) {
            super(iter);
            assert conn != null;
            this.conn = conn;
        }

        @Override
        public void handleClose() throws X {
            super.handleClose();

            try {
                if (conn != null && conn.isOpen())
                    conn.close();
            } catch (RepositoryException e) {

            }
        }
    }


}
