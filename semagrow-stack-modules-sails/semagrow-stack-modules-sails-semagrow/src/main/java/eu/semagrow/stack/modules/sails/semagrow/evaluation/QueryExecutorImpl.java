package eu.semagrow.stack.modules.sails.semagrow.evaluation;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.*;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;

import java.util.Iterator;
import java.util.List;

/**
 * Created by angel on 6/6/14.
 */
public class QueryExecutorImpl implements QueryExecutor {

    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(URI endpoint, TupleExpr expr, BindingSet bindings) throws QueryEvaluationException {

        try {
            RepositoryConnection conn = getConnection(endpoint);
            String sparqlQuery = buildSPARQLQuery(expr);
            TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);

            Iterator<Binding> bIter = bindings.iterator();
            while (bIter.hasNext()) {
                Binding b = bIter.next();
                query.setBinding(b.getName(), b.getValue());
            }

            return query.evaluate();
        } catch (Exception e) {
            throw new QueryEvaluationException(e);
        }
    }

    public CloseableIteration<BindingSet, QueryEvaluationException>
        evaluate(URI endpoint, TupleExpr expr,
                 CloseableIteration<BindingSet, QueryEvaluationException> bindingIter)
            throws QueryEvaluationException {
        return null;
    }


    public RepositoryConnection getConnection(URI endpoint) {
        return null;
    }


    private CloseableIteration<BindingSet, QueryEvaluationException>
        sendQuery(URI endpoint, String sparqlQuery, BindingSet bindings)
            throws QueryEvaluationException, MalformedQueryException, RepositoryException {

        RepositoryConnection conn = getConnection(endpoint);
        TupleQuery query = conn.prepareTupleQuery(QueryLanguage.SPARQL, sparqlQuery);

        for (Binding b : bindings)
            query.setBinding(b.getName(), b.getValue());

        return query.evaluate();
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
    private String buildVALUESClause(List<BindingSet> bindings, List<String> relevantBindingNames)
            throws QueryEvaluationException
    {

        StringBuilder sb = new StringBuilder();
        sb.append(" VALUES (?__rowIdx"); // __rowIdx: see comment in evaluate()

        for (String bName : relevantBindingNames) {
            sb.append(" ?").append(bName);
        }

        sb.append(") { ");

        int rowIdx = 0;
        for (BindingSet b : bindings) {
            sb.append(" (");
            sb.append("\"").append(rowIdx++).append("\" "); // identification of the row for post processing
            for (String bName : relevantBindingNames) {
                appendValueAsString(sb, b.getValue(bName)).append(" ");
            }
            sb.append(")");
        }

        sb.append(" }");
        return sb.toString();
    }

    private String buildSPARQLQuery(TupleExpr expr) throws Exception {
        SPARQLQueryRenderer renderer = new SPARQLQueryRenderer();
        ParsedTupleQuery query = new ParsedTupleQuery(expr);
        return renderer.render(query);
    }

    private String buildSPARQLQueryVALUES(TupleExpr expr, List<BindingSet> bindings, List<String> relevantBindingNames)
            throws Exception {

        return buildSPARQLQuery(expr) + buildVALUESClause(bindings,relevantBindingNames);
    }


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
            sb.append("^^<");
            sb.append(lit.getDatatype().stringValue());
            sb.append('>');
        //}
        return sb;
    }
}
