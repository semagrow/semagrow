package org.semagrow.plan.rel.semagrow;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.semagrow.plan.rel.TupleExprToRelConverter;

/**
 * Created by angel on 18/7/2017.
 */
public abstract class Prepare {

    private final Convention resultConvention;

    Prepare(Convention resultConvention) {
        this.resultConvention = resultConvention;
    }

    protected RelRoot optimize(RelRoot root) {

        final RelOptPlanner planner = root.rel.getCluster().getPlanner();

        final RelTraitSet desiredTraits = getDesiredTraitSet(root);
        final Program program = getProgram();

        RelNode rel = program.run(planner, root.rel, desiredTraits, ImmutableList.of(), ImmutableList.of());

        return root.withRel(rel);
    }

    protected Program getProgram() {
        // FIXME : change me
        return Programs.standard();
    }

    protected RelTraitSet getDesiredTraitSet(RelRoot root) {
        return root.rel.getTraitSet()
                .replace(resultConvention)
                .replace(root.collation)
                .simplify();
    }

    public PreparedResult prepare(QueryRoot expr) {

        TupleExprToRelConverter relConverter = getTupleExprToRelConverter();
        RelRoot root = relConverter.convertQuery(expr);
        RelRoot rootOpt = optimize(root);

        return new PreparedResultImpl(rootOpt);
    }

    protected abstract TupleExprToRelConverter getTupleExprToRelConverter();

    interface PreparedResult {
        RelNode rootRel();
    }

    static class PreparedResultImpl implements PreparedResult {

        final private RelRoot root;

        private PreparedResultImpl(RelRoot root) {
            this.root = root;
        }

        @Override
        public RelNode rootRel() { return root.rel; }
    }

}
