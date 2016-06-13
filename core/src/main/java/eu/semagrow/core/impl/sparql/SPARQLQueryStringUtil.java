package eu.semagrow.core.impl.sparql;

import eu.semagrow.commons.PlainLiteral;
import eu.semagrow.core.impl.evalit.iteration.InsertValuesBindingsIteration;
import eu.semagrow.core.plan.Plan;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.helpers.AbstractQueryModelVisitor;
import org.eclipse.rdf4j.query.parser.ParsedBooleanQuery;
import org.eclipse.rdf4j.query.parser.ParsedTupleQuery;
import org.eclipse.rdf4j.query.parser.sparql.SPARQLUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;

/**
 * Various static functions for query handling.
 *
 * @author Angelos Charalambidis
 * @author Antonis Troumpoukis
 */

public class SPARQLQueryStringUtil {


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

    /**
     * Construct a SPARQL query string for the provided tuple exprossion.
     * If the projection is empty or null, build an ASK query, otherwise build a SELECT query.
     *
     * @param expr
     * @param projection
     * @return The corresponding SPARQL query string
     * @throws Exception
     */


    public static String buildSPARQLQuery(TupleExpr expr, Collection<String> projection)
            throws Exception
    {
        if (projection != null && projection.isEmpty())
            return buildAskSPARQLQuery(expr);
        else
            return buildSelectSPARQLQuery(expr, projection);
    }


    private static String buildSelectSPARQLQuery(TupleExpr expr, Collection<String> projection)
            throws Exception
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

        String queryString = new SPARQLQueryRenderer().render(new ParsedTupleQuery(expr));
        queryString = updateFunctionCallsSELECT(expr, queryString, computeVars(expr));

        return queryString;
    }

    private static String buildAskSPARQLQuery(TupleExpr expr) throws Exception
    {
        ParsedBooleanQuery query = new ParsedBooleanQuery(expr);
        return new SPARQLQueryRenderer().render(query);
    }

    /**
     * Construct a bind join subquery for the provided tuple exprossion and a set of relevant input bindings,
     * using the SPARQL 1.1 VALUES operator.
     *
     * @param expr
     * @param bindings
     * @param relevantBindingNames
     * @return The corresponding SPARQL query string
     * @throws Exception
     */

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

    /**
     * Construct a bind join subquery for the provided tuple exprossion and a set of relevant input bindings,
     * using the UNION operator.
     *
     * @param expr
     * @param bindings
     * @param relevantBindingNames
     * @return The corresponding SPARQL query string
     * @throws Exception
     */

    public static String buildSPARQLQueryUNION(TupleExpr expr,
                                           List<BindingSet> bindings,
                                           Collection<String> relevantBindingNames)
            throws Exception
    {

        Set<String> freeVars = computeVars(expr);
        freeVars.removeAll(relevantBindingNames);

        if (freeVars.isEmpty()) {
            return buildSPARQLQueryUNIONFILTER(expr, bindings, relevantBindingNames);
        }

        String query = new SPARQLQueryRenderer().render(new ParsedTupleQuery(expr));
        query = updateFunctionCallsSELECT(expr, query, freeVars);
        freeVars.addAll(additionalBindingNames(expr));
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
                                                      Collection<String> relevantBindingNames)
            throws Exception
    {
        String query = new SPARQLQueryRenderer().render(new ParsedTupleQuery(expr));
        query = updateFunctionCallsBIND(expr, query); // not tested
        relevantBindingNames.addAll(additionalBindingNames(expr)); // not tested
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

    private static String updateFunctionCallsBIND(TupleExpr expr, String query) {

        String newString = query;
        StringBuilder builder = new StringBuilder();

        if (expr instanceof Plan) {
            TupleExpr e = ((Plan) expr).getArg();
            if (e instanceof Extension) {
                for (ExtensionElem elem : ((Extension) e).getElements()) {
                    ValueExpr f = elem.getExpr();
                    if (f instanceof FunctionCall) {

                        builder.append("  BIND(<" + ((FunctionCall) f).getURI() + ">(");
                        boolean flag = false;
                        for (ValueExpr arg : ((FunctionCall) f).getArgs()) {

                            if (flag) {
                                builder.append(", ");
                            }
                            if (arg instanceof Var) {
                                builder.append("?" + ((Var) arg).getName());
                            }
                            if (arg instanceof ValueConstant) {
                                appendValueAsString(builder, ((ValueConstant) arg).getValue());
                            }
                            flag = true;
                        }
                        builder.append(") as ?" + elem.getName() + ") .\n");
                    }
                }
            }
        }

        newString = newString.replace("}", builder.toString() + "}");
        return newString;
    }

    private static String updateFunctionCallsSELECT(TupleExpr expr, String query, Set<String> freeVars) {

        Boolean extensionFlag = false;

        String newString = query;
        StringBuilder builder = new StringBuilder();

        builder.append("\nSELECT ");
        for (String var : freeVars) {
            builder.append("?"+var+" ");
        }

        if (expr instanceof Plan) {
            TupleExpr e = ((Plan) expr).getArg();
            if (e instanceof Extension) {
                extensionFlag = true;
                for (ExtensionElem elem : ((Extension) e).getElements()) {
                    ValueExpr f = elem.getExpr();
                    if (f instanceof FunctionCall) {

                        builder.append("(<" + ((FunctionCall) f).getURI() + ">(");
                        boolean flag = false;
                        for (ValueExpr arg : ((FunctionCall) f).getArgs()) {

                            if (flag) {
                                builder.append(", ");
                            }
                            if (arg instanceof Var) {
                                builder.append("?" + ((Var) arg).getName());
                            }
                            if (arg instanceof ValueConstant) {
                                appendValueAsString(builder, ((ValueConstant) arg).getValue());
                            }
                            flag = true;
                        }
                        builder.append(") as ?" + elem.getName() + ")");
                    }
                }
            }
        }
        if (extensionFlag) {
            builder.append("\nWHERE {");
            newString = newString.replace("{", "{" + builder.toString());
            newString = newString + "\n}";
        }
        return newString;
    }

    private static Set<String> additionalBindingNames(TupleExpr expr) {

        Set<String> set = new HashSet<>();

        if (expr instanceof Plan) {
            TupleExpr e = ((Plan) expr).getArg();
            if (e instanceof Extension) {
                for (ExtensionElem elem : ((Extension) e).getElements()) {
                    ValueExpr f = elem.getExpr();
                    if (f instanceof FunctionCall) {
                        set.add(elem.getName());
                    }
                }
            }
        }
        return set;
    }

    public static String tupleExpr2Str(TupleExpr expr) {
        try {
            return new SPARQLQueryRenderer().render(new ParsedTupleQuery(expr)).substring(15).replace('\n',' ');
        } catch (Exception e) {
            e.printStackTrace();
            return "";
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////


    private static StringBuilder appendValueAsString(StringBuilder sb, Value value)
    {
        return sb.append(getReplacement(value));
        /*
        // TODO check if there is some convenient method in Sesame!

        if (value == null)
            return sb.append("UNDEF"); // see grammar for BINDINGs def

        else if (value instanceof IRI)
            return appendURI(sb, (IRI)value);

        else if (value instanceof Literal)
            return appendLiteral(sb, (Literal)value);

        // XXX check for other types ? BNode ?
        throw new RuntimeException("Type not supported: " + value.getClass().getCanonicalName());
        */
    }


    /**
     * Append the uri to the stringbuilder, i.e. <uri.stringValue>.
     *
     * @param sb
     * @param uri
     * @return the StringBuilder, for convenience
     */
    private static StringBuilder appendURI(StringBuilder sb, IRI uri)
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



    // TODO maybe add BASE declaration here as well?

    /**
     * Retrieve a modified queryString into which all bindings of the given
     * argument are replaced.
     *
     * @param queryString
     * @param bindings
     * @return the modified queryString
     */
    public static String getQueryString(String queryString, BindingSet bindings) {
        if (bindings.size() == 0) {
            return queryString;
        }

        String qry = queryString;
        int b = qry.indexOf('{');
        String select = qry.substring(0, b);
        String where = qry.substring(b);
        for (String name : bindings.getBindingNames()) {
            String replacement = getReplacement(bindings.getValue(name));
            if (replacement != null) {
                String pattern = "[\\?\\$]" + name + "(?=\\W)";
                select = select.replaceAll(pattern, "(" + Matcher.quoteReplacement(replacement) + " as ?" + name
                        + ")");

                // we use Matcher.quoteReplacement to make sure things like newlines
                // in literal values
                // are preserved
                where = where.replaceAll(pattern, Matcher.quoteReplacement(replacement));
            }
        }
        return select + where;
    }

    private static String getReplacement(Value value) {
        StringBuilder sb = new StringBuilder();
        if (value instanceof IRI) {
            return appendValue(sb, (IRI)value).toString();
        }
        else if (value instanceof PlainLiteral) {
            return appendValue(sb, (PlainLiteral)value).toString();
        }
        else if (value instanceof Literal) {
            return appendValue(sb, (Literal)value).toString();
        }
        else {
            throw new IllegalArgumentException("BNode references not supported by SPARQL end-points");
        }
    }

    private static StringBuilder appendValue(StringBuilder sb, IRI uri) {
        sb.append("<").append(uri.stringValue()).append(">");
        return sb;
    }

    private static StringBuilder appendValue(StringBuilder sb, Literal lit) {
        sb.append('"');
        sb.append(SPARQLUtil.encodeString(lit.getLabel()));
        sb.append('"');

        if (Literals.isLanguageLiteral(lit)) {
            sb.append('@');
            sb.append(lit.getLanguage().get());
        }
        else {
            sb.append("^^<");
            sb.append(lit.getDatatype().stringValue());
            sb.append('>');
        }
        return sb;
    }


    private static StringBuilder appendValue(StringBuilder sb, PlainLiteral lit) {
        sb.append('"');
        sb.append(SPARQLUtil.encodeString(lit.getLabel()));
        sb.append('"');

        return sb;
    }
}
