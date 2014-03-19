package eu.semagrow.stack.modules.querydecomp.optimizer;

import org.openrdf.query.BindingSet;
import org.openrdf.query.Dataset;
import org.openrdf.query.algebra.Join;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.helpers.StatementPatternCollector;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by angel on 3/13/14.
 */
public class DynamicProgrammingOptimizer implements QueryOptimizer {

    public DynamicProgrammingOptimizer() {

    }

    public List<TupleExpr> getBaseExpressions(TupleExpr expr) {

        List<StatementPattern> statementPatterns = StatementPatternCollector.process(expr);

        return null;
    }


    public void optimize(TupleExpr tupleExpr, Dataset dataset, BindingSet bindings) {

    }

    protected <L extends List<TupleExpr>> L getJoinArgs(TupleExpr tupleExpr, L joinArgs) {
        if (tupleExpr instanceof Join) {
            Join join = (Join)tupleExpr;
            getJoinArgs(join.getLeftArg(), joinArgs);
            getJoinArgs(join.getRightArg(), joinArgs);
        }
        else {
            joinArgs.add(tupleExpr);
        }

        return joinArgs;
    }

}
