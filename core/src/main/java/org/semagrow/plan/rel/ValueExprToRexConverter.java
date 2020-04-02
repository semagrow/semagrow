package org.semagrow.plan.rel;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.eclipse.rdf4j.query.algebra.*;
import org.eclipse.rdf4j.query.algebra.BinaryValueOperator;
import org.eclipse.rdf4j.query.algebra.UnaryValueOperator;
import org.eclipse.rdf4j.query.algebra.ValueExpr;
import org.eclipse.rdf4j.query.algebra.evaluation.function.datetime.*;
import org.eclipse.rdf4j.query.algebra.evaluation.function.numeric.*;
import org.eclipse.rdf4j.query.algebra.evaluation.function.string.*;
import org.eclipse.rdf4j.query.algebra.evaluation.function.rdfterm.*;
import org.semagrow.plan.rel.sparql.SparqlStdOperatorTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by angel on 4/7/2017.
 */
public class ValueExprToRexConverter {

    private RexBuilder rexBuilder;

    Map<Class<?>, SqlOperator> ops = new HashMap<>();

    public ValueExprToRexConverter(RelOptCluster cluster) {

        rexBuilder = cluster.getRexBuilder();

        registerOp(And.class, SparqlStdOperatorTable.AND);
        registerOp(Or.class,  SparqlStdOperatorTable.OR);
        registerOp(Not.class, SparqlStdOperatorTable.NOT);

        registerOp(Abs.class, SparqlStdOperatorTable.ABS);
        registerOp(Ceil.class, SparqlStdOperatorTable.CEIL);
        registerOp(Floor.class, SparqlStdOperatorTable.FLOOR);
        registerOp(Rand.class, SparqlStdOperatorTable.RAND);
        registerOp(Round.class, SparqlStdOperatorTable.ROUND);

        registerOp(Like.class, SparqlStdOperatorTable.LIKE);
        registerOp(Coalesce.class, SparqlStdOperatorTable.COALESCE);
        registerOp(Bound.class, SparqlStdOperatorTable.BOUND);
        registerOp(IsNumeric.class, SparqlStdOperatorTable.IS_NUMERIC);
        registerOp(IsLiteral.class, SparqlStdOperatorTable.IS_LITERAL);
        registerOp(IsResource.class, SparqlStdOperatorTable.IS_RESOURCE);
        registerOp(IsBNode.class, SparqlStdOperatorTable.BNODE);
        registerOp(IsURI.class, SparqlStdOperatorTable.IS_URI);
        registerOp(LangMatches.class, SparqlStdOperatorTable.LANG_MATCHES);
        registerOp(Lang.class, SparqlStdOperatorTable.LANG);
        registerOp(Label.class, SparqlStdOperatorTable.LABEL);
        registerOp(Datatype.class, SparqlStdOperatorTable.DATATYPE);
        registerOp(Str.class, SparqlStdOperatorTable.STR);
        registerOp(Regex.class, SparqlStdOperatorTable.REGEX);

        registerOp(StrLang.class, SparqlStdOperatorTable.STRLANG);
        registerOp(StrDt.class, SparqlStdOperatorTable.STRDT);
        registerOp(STRUUID.class, SparqlStdOperatorTable.STRUUID);
        registerOp(UUID.class, SparqlStdOperatorTable.UUID);

        registerOp(UpperCase.class, SparqlStdOperatorTable.UCASE);
        registerOp(LowerCase.class, SparqlStdOperatorTable.LCASE);
        registerOp(Substring.class, SparqlStdOperatorTable.SUBSTR);
        registerOp(LowerCase.class, SparqlStdOperatorTable.LCASE);
        registerOp(StrAfter.class, SparqlStdOperatorTable.STRAFTER);
        registerOp(StrBefore.class, SparqlStdOperatorTable.STRBEFORE);
        registerOp(StrEnds.class, SparqlStdOperatorTable.STRENDS);
        registerOp(StrStarts.class, SparqlStdOperatorTable.STRSTARTS);
        registerOp(StrLen.class, SparqlStdOperatorTable.STRLEN);
        registerOp(Contains.class, SparqlStdOperatorTable.CONTAINS);
        registerOp(Concat.class, SparqlStdOperatorTable.CONCAT);
        registerOp(Replace.class, SparqlStdOperatorTable.REPLACE);
        registerOp(EncodeForUri.class, SparqlStdOperatorTable.ENCODE_FOR_URI);

        registerOp(If.class, SparqlStdOperatorTable.IF);

        registerOp(Hours.class, SparqlStdOperatorTable.HOURS);
        registerOp(Day.class, SparqlStdOperatorTable.DAY);
        registerOp(Now.class, SparqlStdOperatorTable.NOW);
        registerOp(Month.class, SparqlStdOperatorTable.MONTH);
        registerOp(Year.class, SparqlStdOperatorTable.YEAR);
        registerOp(Minutes.class, SparqlStdOperatorTable.MINUTES);
        registerOp(Seconds.class, SparqlStdOperatorTable.SECONDS);
        registerOp(Timezone.class, SparqlStdOperatorTable.TIMEZONE);
        registerOp(Tz.class, SparqlStdOperatorTable.TZ);

        registerOp(Count.class, SparqlStdOperatorTable.COUNT);
        registerOp(Sum.class, SparqlStdOperatorTable.SUM);
        registerOp(Avg.class, SparqlStdOperatorTable.AVG);
        registerOp(Max.class, SparqlStdOperatorTable.MAX);
        registerOp(Min.class, SparqlStdOperatorTable.MIN);
        registerOp(Sample.class, SparqlStdOperatorTable.SAMPLE);
        registerOp(GroupConcat.class, SparqlStdOperatorTable.GROUPCONCAT);

        registerCompareOp(Compare.class, Compare.CompareOp.GT, SparqlStdOperatorTable.GREATER_THAN);
        registerCompareOp(Compare.class, Compare.CompareOp.GE, SparqlStdOperatorTable.GREATER_THAN_OR_EQUAL);
        registerCompareOp(Compare.class, Compare.CompareOp.LT, SparqlStdOperatorTable.LESS_THAN);
        registerCompareOp(Compare.class, Compare.CompareOp.LE, SparqlStdOperatorTable.LESS_THAN_OR_EQUAL);
        registerCompareOp(Compare.class, Compare.CompareOp.EQ, SparqlStdOperatorTable.EQUALS);
        registerOp(SameTerm.class, SparqlStdOperatorTable.SAMETERM);

        registerMathOp(MathExpr.class, MathExpr.MathOp.PLUS, SparqlStdOperatorTable.PLUS);
        registerMathOp(MathExpr.class, MathExpr.MathOp.MINUS, SparqlStdOperatorTable.MINUS);
        registerMathOp(MathExpr.class, MathExpr.MathOp.MULTIPLY, SparqlStdOperatorTable.MULTIPLY);
        registerMathOp(MathExpr.class, MathExpr.MathOp.DIVIDE, SparqlStdOperatorTable.DIVIDE);
    }


    private void registerOp(Class<?> clazz, SqlOperator op) {
        ops.put(clazz, op);
    }

    private void registerCompareOp(Class<?> clazz, Compare.CompareOp cmpop, SqlOperator op) {
        //ops.put(clazz, op);
    }

    private void registerMathOp(Class<?> clazz, MathExpr.MathOp mathop,  SqlOperator op) {
        //ops.put(clazz, op);
    }

    public RexNode convertExpr(ValueExpr e, ValueExprConverterContext ctx) {

        if (e instanceof CompareAll) {
            return convertScalar((CompareAll)e, ctx);
        } else if (e instanceof If) {
            return  null; //convertIf((If)e, ctx);
        } else if (e instanceof Compare) {
            return convertCompare((Compare) e, ctx);
        } else if (e instanceof MathExpr) {
            return convertMath((MathExpr) e, ctx);
        } else if (e instanceof FunctionCall) {
            return convertUDFCall((FunctionCall)e, ctx);
        } else if (isCall(e)) {
            return convertCall(e, ctx);
        } else
            return ctx.convertExpression(e);
    }

    private RexNode convertScalar(CompareAll e, ValueExprConverterContext ctx) {
        RexNode q   = ctx.convertSubQuery(e.getSubQuery());
        RexNode arg = convertExpr(e.getArg(), ctx);
        SqlOperator op = convertCompareOp(e.getOperator());
        return rexBuilder.makeCall(op, arg, q);
    }

    private RexNode convertCompare(Compare e, ValueExprConverterContext ctx) {
        SqlOperator op = convertCompareOp(e.getOperator());
        RexNode lhs = convertExpr(e.getLeftArg(), ctx);
        RexNode rhs = convertExpr(e.getRightArg(), ctx);
        return rexBuilder.makeCall(op, lhs, rhs);
    }

    private RexNode convertMath(MathExpr e, ValueExprConverterContext ctx) {
        SqlOperator op = convertMathOp(e.getOperator());
        RexNode lhs = convertExpr(e.getLeftArg(), ctx);
        RexNode rhs = convertExpr(e.getRightArg(), ctx);
        return rexBuilder.makeCall(op, lhs, rhs);
    }

    private RexNode convertUDFCall(FunctionCall e, ValueExprConverterContext ctx) {
        List<RexNode> args = Lists.transform(e.getArgs(), ee -> convertExpr(ee, ctx));
        SqlOperator op = SparqlStdOperatorTable.makeSparqlFunction(e.getURI(), args.size());
        return rexBuilder.makeCall(op, args);
    }

    private RexNode convertCall(ValueExpr e, ValueExprConverterContext ctx) {
        Class<?> f = functor(e);
        SqlOperator op = ops.get(f);
        if (op != null) {
            List<RexNode> args = Lists.transform(args(e), a -> convertExpr(a, ctx));
            return rexBuilder.makeCall(op, args);
        } else
            return null;
    }

    private SqlOperator convertCompareOp(Compare.CompareOp operator) {
        switch (operator) {
            case EQ : return SqlStdOperatorTable.EQUALS;
            case GE : return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
            case GT : return SqlStdOperatorTable.GREATER_THAN;
            case LE : return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
            case LT : return SqlStdOperatorTable.LESS_THAN;
        }
        return null;
    }

    private SqlOperator convertMathOp(MathExpr.MathOp operator) {
        switch (operator) {
            case PLUS : return SqlStdOperatorTable.PLUS;
            case MULTIPLY : return SqlStdOperatorTable.MULTIPLY;
            case DIVIDE : return SqlStdOperatorTable.DIVIDE;
            case MINUS : return SqlStdOperatorTable.MINUS;
        }
        return null;
    }

    private boolean isCall(ValueExpr e) {
        return (e instanceof UnaryValueOperator ||
                e instanceof BinaryValueOperator ||
                e instanceof If ||
                e instanceof NAryValueOperator);
    }


    private Class<?> functor(ValueExpr e) {
        if (isCall(e))
            return e.getClass();
        else
            return null;
    }

    private List<ValueExpr> args(ValueExpr e) {
        ImmutableList.Builder<ValueExpr> args = ImmutableList.builder();
        if (e instanceof UnaryValueOperator) {
            args.add(((UnaryValueOperator) e).getArg());
        } else if (e instanceof BinaryValueOperator) {
            args.add(((BinaryValueOperator) e).getLeftArg());
            args.add(((BinaryValueOperator) e).getRightArg());
        } else if (e instanceof If) {
            args.add(((If)e).getCondition());
            args.add(((If)e).getResult());
            args.add(((If)e).getAlternative());
        } else if (e instanceof NAryValueOperator) {
            args.addAll(((NAryValueOperator)e).getArguments());
        }
        return args.build();
    }


    interface ValueExprConverterContext {
        RexNode convertExpression(ValueExpr e);
        RexNode convertSubQuery(TupleExpr e);
    }
}
