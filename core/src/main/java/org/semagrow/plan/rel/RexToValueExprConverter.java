package org.semagrow.plan.rel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.NlsString;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.Var;
import org.semagrow.plan.rel.type.RDFTypes;

import java.util.UUID;

/**
 * Created by antonis on 10/7/2017.
 */
public class RexToValueExprConverter extends RexVisitorImpl<ValueExpr> {

    private Context context;
    private ValueFactory vf = SimpleValueFactory.getInstance();

    protected RexToValueExprConverter(Context context) {
        super(false);
        this.context = context;
    }

    static public ValueExpr convertExpression(RexNode rex, Context ctx)
    {
        RexToValueExprConverter converter = new RexToValueExprConverter(ctx);
        return rex.accept(converter);
    }

    static public Value convertLiteral(RexLiteral rex)
    {
        RexToValueExprConverter converter = new RexToValueExprConverter(null);
        return converter.convertRexLiteral(rex);
    }

    private Value convertRexLiteral(RexLiteral literal) {

        if (literal.getType().equals(RDFTypes.BLANK_NODE)) {
            return null;
        }
        if (literal.getType().equals(RDFTypes.IRI_TYPE)) {
            return convertIri(literal.getValue());
        }
        if (literal.getType().equals(RDFTypes.PLAIN_LITERAL)) {
            return convertLiteral(literal.getValue());
        }
        return null;
    }

    private Value convertLiteral(Comparable value) {
        return null;
    }

    private Value convertIri(Comparable value) {
        if (value instanceof NlsString) {
            return vf.createIRI(((NlsString) value).getValue());
        } else {
            return vf.createIRI(value.toString());
        }
    }


    @Override
    public ValueExpr visitLiteral(RexLiteral lit) {
        String const_name = "const-" + UUID.randomUUID().toString();
        return new Var(const_name, convertLiteral(lit));
    }

    @Override
    public ValueExpr visitCall(RexCall call) {
        return null;
        //Lists.transform(call.getOperands();
        //call.getOperator();
    }

    @Override
    public ValueExpr visitInputRef(RexInputRef inputRef) {
        return context.field(inputRef.getIndex());
    }

    @Override
    public ValueExpr visitCorrelVariable(RexCorrelVariable correlVariable) {
        return null;
    }

    ValueExpr convertRexNode(RexNode node, RelDataType rowtype) {
        if (node instanceof RexCall) {
            return convertRexCall((RexCall) node, rowtype);
        }
        return null;
    }

    ValueExpr convertRexCall(RexCall node, RelDataType rowtype) {
        if (node.getOperator() instanceof SqlBinaryOperator) {
            return convertSqlBinaryOperator(node, rowtype);
        }
        return null;
    }

    ValueExpr convertSqlBinaryOperator(RexCall node, RelDataType rowtype) {

        if (node.getOperator().equals(SqlStdOperatorTable.EQUALS)) {

        }
        if (node.getOperator().equals(SqlStdOperatorTable.GREATER_THAN)) {

        }
        if (node.getOperator().equals(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL)) {

        }
        if (node.getOperator().equals(SqlStdOperatorTable.LESS_THAN)) {

        }
        if (node.getOperator().equals(SqlStdOperatorTable.LESS_THAN_OR_EQUAL)) {

        }
        if (node.getOperator().equals(SqlStdOperatorTable.NOT_EQUALS)) {

        }
        return null;
    }

    /*
    public static final SqlBinaryOperator CONCAT;
    public static final SqlBinaryOperator DIVIDE;
    public static final SqlBinaryOperator DIVIDE_INTEGER;
    public static final SqlBinaryOperator DOT;
    public static final SqlBinaryOperator EQUALS;
    public static final SqlBinaryOperator GREATER_THAN;
    public static final SqlBinaryOperator IS_DISTINCT_FROM;
    public static final SqlBinaryOperator IS_NOT_DISTINCT_FROM;
    public static final SqlBinaryOperator IS_DIFFERENT_FROM;
    public static final SqlBinaryOperator GREATER_THAN_OR_EQUAL;
    public static final SqlBinaryOperator IN;
    public static final SqlBinaryOperator NOT_IN;
    public static final SqlBinaryOperator LESS_THAN;
    public static final SqlBinaryOperator LESS_THAN_OR_EQUAL;
    public static final SqlBinaryOperator MINUS;
    public static final SqlBinaryOperator MULTIPLY;
    public static final SqlBinaryOperator NOT_EQUALS;
    public static final SqlBinaryOperator OR;
    public static final SqlBinaryOperator PLUS;
    */


    public interface Context {
        ValueExpr field(int ordinal);
    }

}
