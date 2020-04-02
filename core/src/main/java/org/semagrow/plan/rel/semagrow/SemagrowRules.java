package org.semagrow.plan.rel.semagrow;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Set;

public class SemagrowRules {

    static RelOptRule SEMAGROW_JOIN_RULE = new SemagrowJoinRule();
    static RelOptRule SEMAGROW_MERGE_JOIN_RULE = new SemagrowMergeJoinRule();
    static RelOptRule SEMAGROW_PROJECT_RULE = new SemagrowProjectRule();
    static RelOptRule SEMAGROW_FILTER_RULE = new SemagrowFilterRule();
    static RelOptRule SEMAGROW_AGGREGATE_RULE = new SemagrowAggregateRule();
    static RelOptRule SEMAGROW_SORT_RULE = new SemagrowSortRule();
    static RelOptRule SEMAGROW_INTERSECT_RULE = new SemagrowIntersectRule();
    static RelOptRule SEMAGROW_UNION_RULE = new SemagrowUnionRule();
    static RelOptRule SEMAGROW_VALUES_RULE = new SemagrowValuesRule();
    static RelOptRule SEMAGROW_MINUS_RULE = new SemagrowMinusRule();

    public static List<RelOptRule> RULES =
        ImmutableList.of(
                SEMAGROW_JOIN_RULE,
                //SEMAGROW_MERGE_JOIN_RULE,
                SEMAGROW_PROJECT_RULE,
                SEMAGROW_FILTER_RULE,
                SEMAGROW_AGGREGATE_RULE,
                SEMAGROW_SORT_RULE,
                SEMAGROW_UNION_RULE,
                SEMAGROW_INTERSECT_RULE,
                SEMAGROW_MINUS_RULE,
                SEMAGROW_VALUES_RULE);

    public static abstract class SemagrowConverterRule extends ConverterRule {

        public SemagrowConverterRule(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String description) {
            super(clazz, in, out, description);
        }

        public <R extends RelNode> SemagrowConverterRule(Class<R> clazz, Predicate<? super R> predicate, RelTrait in, RelTrait out, String description) {
            super(clazz, predicate, in, out, description);
        }
    }

    public static class SemagrowJoinRule extends SemagrowConverterRule {

        public SemagrowJoinRule() {
            super(LogicalJoin.class, Convention.NONE, SemagrowConvention.INSTANCE, "SemagrowJoinRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalJoin join = (LogicalJoin)rel;
            return new SemagrowJoin(join.getCluster(),
                    join.getTraitSet().replace(getOutConvention()),
                    convert(join.getLeft(), getOutTrait()),
                    convert(join.getRight(), getOutTrait()),
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType());
        }
    }

    public static class SemagrowJoin extends Join implements SemagrowRel {

        protected SemagrowJoin(RelOptCluster cluster,
                               RelTraitSet traitSet,
                               RelNode left,
                               RelNode right,
                               RexNode condition,
                               Set<CorrelationId> variablesSet,
                               JoinRelType joinType)
        {
            super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        }

        @Override
        public SemagrowJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
            return new SemagrowJoin(getCluster(), traitSet, left, right, conditionExpr, ImmutableSet.of(), joinType);
        }
    }

    public static class SemagrowMergeJoinRule extends SemagrowConverterRule {

        public SemagrowMergeJoinRule() {
            super(LogicalJoin.class, Convention.NONE, SemagrowConvention.INSTANCE, "SemagrowMergeJoinRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalJoin join = (LogicalJoin)rel;

            final JoinInfo info =
                    JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());

            if (join.getJoinType() != JoinRelType.INNER) {
                // EnumerableMergeJoin only supports inner join.
                // (It supports non-equi join, using a post-filter; see below.)
                return null;
            }

            if (info.pairs().size() == 0) {
                // EnumerableMergeJoin CAN support cartesian join, but disable it for now.
                return null;
            }

            final List<RelNode> newInputs = Lists.newArrayList();
            final List<RelCollation> collations = Lists.newArrayList();

            int offset = 0;
            for (Ord<RelNode> ord : Ord.zip(join.getInputs())) {
                RelTraitSet traits = ord.e.getTraitSet()
                        .replace(getOutConvention());
                if (!info.pairs().isEmpty()) {
                    final List<RelFieldCollation> fieldCollations = Lists.newArrayList();
                    for (int key : info.keys().get(ord.i)) {
                        fieldCollations.add(
                                new RelFieldCollation(key,
                                        RelFieldCollation.Direction.ASCENDING,
                                        RelFieldCollation.NullDirection.LAST));
                    }
                    final RelCollation collation = RelCollations.of(fieldCollations);
                    collations.add(RelCollations.shift(collation, offset));
                    traits = traits.replace(collation);
                }
                newInputs.add(convert(ord.e, traits));
                offset += ord.e.getRowType().getFieldCount();
            }
            final RelNode left = newInputs.get(0);
            final RelNode right = newInputs.get(1);
            final RelOptCluster cluster = join.getCluster();

            RelNode newRel;

            RelTraitSet traits = join.getTraitSet().replace(getOutConvention());
            if (!collations.isEmpty()) {
                traits = traits.replace(collations);
            }

            newRel = new SemagrowMergeJoin(join.getCluster(),
                    join.getTraitSet().replace(getOutConvention()),
                    convert(join.getLeft(), getOutTrait()),
                    convert(join.getRight(), getOutTrait()),
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType());


            if (!info.isEqui()) {
                newRel = new SemagrowFilter(cluster, newRel.getTraitSet(),
                        newRel, info.getRemaining(cluster.getRexBuilder()));
            }
            return newRel;

        }
    }

    public static class SemagrowMergeJoin extends Join implements SemagrowRel {

        protected SemagrowMergeJoin(RelOptCluster cluster,
                               RelTraitSet traitSet,
                               RelNode left,
                               RelNode right,
                               RexNode condition,
                               Set<CorrelationId> variablesSet,
                               JoinRelType joinType)
        {
            super(cluster, traitSet, left, right, condition, variablesSet, joinType);
        }

        @Override
        public SemagrowJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
            return new SemagrowJoin(getCluster(), traitSet, left, right, conditionExpr, ImmutableSet.of(), joinType);
        }

    }

    public static class SemagrowProjectRule extends SemagrowConverterRule {

        SemagrowProjectRule() {
            super(LogicalProject.class, Convention.NONE, SemagrowConvention.INSTANCE, "SemagrowProjectRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalProject project = (LogicalProject) rel;
            return new SemagrowProject(project.getCluster(),
                    project.getTraitSet().replace(getOutConvention()),
                    convert(project.getInput(), getOutTrait()),
                    project.getProjects(),
                    project.getRowType());
        }
    }

    public static class SemagrowProject extends Project implements SemagrowRel {

        protected SemagrowProject(RelOptCluster cluster,
                                  RelTraitSet traits,
                                  RelNode input,
                                  List<? extends RexNode> projects, RelDataType rowType) {
            super(cluster, traits, input, projects, rowType);
        }

        @Override
        public SemagrowProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
            return new SemagrowProject(getCluster(), traitSet, input, projects, rowType);
        }
    }

    public static class SemagrowFilterRule extends SemagrowConverterRule {

        SemagrowFilterRule() {
            super(LogicalFilter.class, Convention.NONE, SemagrowConvention.INSTANCE, "SemagrowFilterRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalFilter filter = (LogicalFilter)rel;
            return new SemagrowFilter(filter.getCluster(),
                    filter.getTraitSet().replace(getOutConvention()),
                    convert(filter.getInput(), getOutTrait()),
                    filter.getCondition()
                    );
        }
    }

    public static class SemagrowFilter extends Filter implements SemagrowRel {

        protected SemagrowFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
            super(cluster, traits, child, condition);
        }

        @Override
        public SemagrowFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
            return new SemagrowFilter(getCluster(), traitSet, input, condition);
        }
    }

    public static class SemagrowAggregateRule extends SemagrowConverterRule {

        SemagrowAggregateRule(){
            super(LogicalAggregate.class, Convention.NONE, SemagrowConvention.INSTANCE, "SemagrowAggregateRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalAggregate aggregate = (LogicalAggregate) rel;
            return new SemagrowAggregate(aggregate.getCluster(),
                    aggregate.getTraitSet().replace(getOutConvention()),
                    convert(aggregate.getInput(), getOutTrait()),
                    aggregate.indicator,
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    aggregate.getAggCallList());
        }
    }

    public static class SemagrowAggregate extends Aggregate implements SemagrowRel {

        protected SemagrowAggregate(RelOptCluster cluster,
                                    RelTraitSet traits,
                                    RelNode child,
                                    boolean indicator,
                                    ImmutableBitSet groupSet,
                                    List<ImmutableBitSet> groupSets,
                                    List<AggregateCall> aggCalls) {
            super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
        }

        @Override
        public SemagrowAggregate copy(RelTraitSet traitSet,
                                      RelNode input,
                                      boolean indicator,
                                      ImmutableBitSet groupSet,
                                      List<ImmutableBitSet> groupSets,
                                      List<AggregateCall> aggCalls) {
            return new SemagrowAggregate(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
        }
    }

    public static class SemagrowSortRule extends SemagrowConverterRule {


        SemagrowSortRule() {
            super(LogicalSort.class, Convention.NONE, SemagrowConvention.INSTANCE, "SemagrowSortRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalSort sort = (LogicalSort) rel;
            return new SemagrowSort(sort.getCluster(),
                    sort.getTraitSet().replace(getOutConvention()),
                    convert(sort.getInput(), getOutTrait()),
                    sort.getCollation(),
                    sort.offset,
                    sort.fetch);
        }
    }

    public static class SemagrowSort extends Sort implements SemagrowRel {

        public SemagrowSort(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
            super(cluster, traits, child, collation, offset, fetch);
        }

        @Override
        public SemagrowSort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
            return new SemagrowSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
        }
    }

    public static class SemagrowIntersectRule extends SemagrowConverterRule {

        SemagrowIntersectRule() {
            super(LogicalIntersect.class, Convention.NONE, SemagrowConvention.INSTANCE, "SemagrowIntersectRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalIntersect intersect = (LogicalIntersect) rel;
            return new SemagrowIntersect(
                    intersect.getCluster(),
                    intersect.getTraitSet().replace(getOutConvention()),
                    convertList(intersect.getInputs(), getOutTrait()),
                    intersect.all);
        }
    }

    public static class SemagrowIntersect extends Intersect implements SemagrowRel {

        public SemagrowIntersect(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
            super(cluster, traits, inputs, all);
        }

        @Override
        public SemagrowIntersect copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
            return new SemagrowIntersect(getCluster(), traitSet, inputs, all);
        }
    }

    public static class SemagrowUnionRule extends SemagrowConverterRule {

        SemagrowUnionRule() {
            super(LogicalUnion.class, Convention.NONE, SemagrowConvention.INSTANCE, "SemagrowUnionRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalUnion intersect = (LogicalUnion) rel;
            return new SemagrowUnion(
                    intersect.getCluster(),
                    intersect.getTraitSet().replace(getOutConvention()),
                    convertList(intersect.getInputs(), getOutTrait()),
                    intersect.all);
        }
    }

    public static class SemagrowUnion extends Union implements SemagrowRel {

        protected SemagrowUnion(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
            super(cluster, traits, inputs, all);
        }

        @Override
        public SemagrowUnion copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
            return new SemagrowUnion(getCluster(), traitSet, inputs, all);
        }
    }


    public static class SemagrowMinusRule extends SemagrowConverterRule {

        SemagrowMinusRule() {
            super(LogicalMinus.class, Convention.NONE, SemagrowConvention.INSTANCE, "SemagrowMinusRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalMinus intersect = (LogicalMinus) rel;
            return new SemagrowMinus(
                    intersect.getCluster(),
                    intersect.getTraitSet().replace(getOutConvention()),
                    convertList(intersect.getInputs(), getOutTrait()),
                    intersect.all);
        }
    }

    public static class SemagrowMinus extends Minus implements SemagrowRel {

        public SemagrowMinus(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
            super(cluster, traits, inputs, all);
        }

        @Override
        public SemagrowMinus copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
            return new SemagrowMinus(getCluster(),
                    traitSet,
                    inputs,
                    all);
        }
    }

    public static class SemagrowValuesRule extends SemagrowConverterRule {
        SemagrowValuesRule() {
            super(LogicalValues.class, Convention.NONE, SemagrowConvention.INSTANCE, "SemagrowValuesRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalValues values = (LogicalValues)rel;
            return new SemagrowValues(values.getCluster(),
                    values.getRowType(),
                    values.tuples,
                    values.getTraitSet().replace(getOutConvention()));
        }
    }


    public static class SemagrowValues extends Values implements SemagrowRel {

        protected SemagrowValues(RelOptCluster cluster,
                                 RelDataType rowType,
                                 ImmutableList<ImmutableList<RexLiteral>> tuples,
                                 RelTraitSet traits) {
            super(cluster, rowType, tuples, traits);
        }
    }
}
