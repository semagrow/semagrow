package eu.semagrow.core.impl.evaluation;

import eu.semagrow.core.impl.evaluation.iteration.InsertValuesBindingsIteration;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.algebra.*;
import org.openrdf.query.algebra.helpers.QueryModelVisitorBase;
import org.openrdf.query.parser.ParsedBooleanQuery;
import org.openrdf.query.parser.ParsedTupleQuery;
import org.openrdf.queryrender.sparql.SPARQLQueryRenderer;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Created by angel on 5/7/2015.
 */
public class SPARQLQueryStringUtils {


    private static boolean rowIdOpt = false;


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
    private static String buildVALUESClause(List<BindingSet> bindings, Set<String> relevantBindingNames)
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


    public static String buildSPARQLQuery(TupleExpr expr, Collection<String> projection)
            throws Exception
    {
        if (projection != null && projection.isEmpty())
            return buildAskSPARQLQuery(expr);
        else
            return buildSelectSPARQLQuery(expr, projection);
    }


    private static String buildSelectSPARQLQuery(TupleExpr expr, Collection<String> projection) throws Exception
    {
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

    private static String buildAskSPARQLQuery(TupleExpr expr) throws Exception
    {
        ParsedBooleanQuery query = new ParsedBooleanQuery(expr);
        return new SPARQLQueryRenderer().render(query);
    }


    public static String buildSPARQLQueryVALUES(TupleExpr expr,
                                         List<BindingSet> bindings,
                                         Set<String> relevantBindingNames)
            throws Exception
    {
        Set<String> freeVars = computeVars(expr);

        if (rowIdOpt)
            freeVars.add(InsertValuesBindingsIteration.INDEX_BINDING_NAME);

        //freeVars.removeAll(relevantBindingNames);

        return buildSPARQLQuery(expr,freeVars) + buildVALUESClause(bindings,relevantBindingNames);
    }

    ////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////

    public static String buildSPARQLQueryUNION(TupleExpr expr,
                                           List<BindingSet> bindings,
                                           Set<String> relevantBindingNames)
            throws Exception
    {

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

    private static String buildSPARQLQueryUNIONFILTER(TupleExpr expr,
                                                      List<BindingSet> bindings,
                                                      Set<String> relevantBindingNames)
            throws Exception
    {
        String query = new SPARQLQueryRenderer().render(new ParsedTupleQuery(expr));
        String where = query.substring(query.indexOf('{'));
        StringBuilder sb = new StringBuilder();
        int i = 1;
        boolean flag1 = false;
        for (BindingSet b : bindings) {
            if (flag1) {
                sb.append(" UNION ");
            }
            flag1 = true;
            String tmpStr = where;
            for (String name : relevantBindingNames) {
                String pattern = "[\\?\\$]" + name + "(?=\\W)";
                String replacement = "?" + name + "_" + i;
                tmpStr = tmpStr.replaceAll(pattern, Matcher.quoteReplacement(replacement));
            }
            tmpStr = tmpStr.substring(0,tmpStr.lastIndexOf('}'));
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
            sb.append(") }");
            i++;
        }
        sb.append(" }");
        String pr = "SELECT ";
        for (int j=1; j<i; j++) {
            for (String name : relevantBindingNames) {
                pr = pr + "?" + name + "_" + j + " ";
            }
        }
        pr = pr + "\nWHERE { ";
        return (pr + sb.toString());
    }
    ////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////


    private static StringBuilder appendValueAsString(StringBuilder sb, Value value)
    {
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
    private static StringBuilder appendURI(StringBuilder sb, URI uri)
    {
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
    private static StringBuilder appendLiteral(StringBuilder sb, Literal lit)
    {
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

    /**
     * Compute the variable names occurring in the service expression using tree
     * traversal, since these are necessary for building the SPARQL query.
     *
     * @return the set of variable names in the given service expression
     */
    private static Set<String> computeVars(TupleExpr serviceExpression) {
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
}
