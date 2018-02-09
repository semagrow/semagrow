package org.semagrow.plan.rel.sparql;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

/**
 * The set of planner rules to convert from Logical to SPARQL AST.
 * The rules implement the logic that decides whether the endpoint can
 * support the Logical node or not depending on the version of SPARQL and
 * the custom function calls that supports (for example, geoSPARQL).
 */
public abstract class SparqlRules {

    protected static final Logger LOGGER = LoggerFactory.getLogger(SparqlRules.class);

    public static List<RelOptRule> rules(SparqlConvention convention) {
        if (convention.getDialect().getVersion() == SparqlVersion.SPARQL11)
            return sparql11(convention);
        else
            return sparql10(convention);
    }

    public static List<RelOptRule> sparql10(SparqlConvention convention) {
        return ImmutableList.of(
                new SparqlJoinRule(convention),
                new SparqlFilterRule(convention),
                new SparqlProjectRule(convention),
                new SparqlAggregateRule(convention),
                new SparqlSortRule(convention),
                new SparqlToSemagrowConverterRule(convention));
    }

    public static List<RelOptRule> sparql11(SparqlConvention convention) {
        return ImmutableList.of(
                new SparqlToSemagrowConverterRule(convention),
                new SparqlJoinRule(convention),
                new SparqlFilterRule(convention),
                new SparqlProjectRule(convention),
                new SparqlAggregateRule(convention),
                new SparqlSortRule(convention),
                new SparqlUnionRule(convention),
                new SparqlIntersectRule(convention),
                new SparqlMinusRule(convention),
                new SparqlValuesRule(convention));
    }

    private static abstract class SparqlConverterRule extends ConverterRule {

        protected final SparqlConvention out;

        public SparqlConverterRule(Class<? extends RelNode> clazz, RelTrait in,
                                   SparqlConvention out, String description) {
            this(clazz, Predicates.<RelNode>alwaysTrue(), in, out, description);
        }

        public <R extends RelNode> SparqlConverterRule(Class<R> clazz,
                                                       Predicate<? super R> predicate,
                                                       RelTrait in, SparqlConvention out,
                                                       String description) {
            super(clazz, predicate, in, out, description);
            this.out = out;
        }

        public boolean isSupported(RexNode node) {
            return node.accept(new SupportedCallsVisitor(out.getDialect()));
        }

        public boolean isSupported(SqlOperator op) {
            return out.getDialect().supportsFeature(SparqlFeature.of(op));
        }

        class SupportedCallsVisitor extends RexVisitorImpl<Boolean> {

            private SparqlDialect dialect;

            protected SupportedCallsVisitor(SparqlDialect dialect) {
                super(true);
                this.dialect = Preconditions.checkNotNull(dialect);
            }

            @Override
            public Boolean visitCall(RexCall call) {
                return isSupported(call.op) && visitChildren(call.getOperands());
            }

            @Override
            public Boolean visitSubQuery(RexSubQuery subQuery) {
                return dialect.supportsSubQuery()
                        && visitChildren(subQuery.getOperands());
            }

            @Override
            public Boolean visitInputRef(RexInputRef inputRef) {
                return true;
            }

            @Override
            public Boolean visitLiteral(RexLiteral literal) {
                return true;
            }

            private Boolean visitChildren(List<RexNode> nodes) {
                for (RexNode o : nodes) {
                    if (!o.accept(this))
                        return false;
                }
                return true;
            }
        }

    }

    private static class SparqlJoinRule extends SparqlConverterRule {

        public SparqlJoinRule(SparqlConvention out) {
            super(LogicalJoin.class, Convention.NONE, out, "SparqlJoinRule");
        }

        @Override
        public RelNode convert(RelNode relNode) {

            LogicalJoin join = (LogicalJoin) relNode;

            JoinInfo joinInfo = JoinInfo.of(join.getLeft(), join.getRight(), join.getCondition());

            if (join.getJoinType() == JoinRelType.FULL || join.getJoinType() == JoinRelType.RIGHT)
                return null; // SPARQL does not support full outer join (yet)

            if (joinInfo.isEqui()) {

                RelNode left = convert(join.getLeft(), getOutTrait());
                RelNode right = convert(join.getRight(), getOutTrait());
                ImmutableIntList leftKeys = joinInfo.leftKeys;
                ImmutableIntList rightKeys = joinInfo.rightKeys;
                JoinRelType joinType = join.getJoinType();

                if (joinType == JoinRelType.RIGHT) {
                    return null;
                    // we can also swap sides and use JoinRelType.LEFT instead but
                    // we should make sure that join.getVariablesSet() is empty, namely
                    // the right side does not depend on the left side.
                }

                return new SparqlEquiJoin(
                        join.getCluster(),
                        join.getTraitSet().replace(getOutTrait()),
                        left,
                        right,
                        joinInfo.getEquiCondition(left, right, join.getCluster().getRexBuilder()),
                        leftKeys,
                        rightKeys,
                        join.getVariablesSet(),
                        joinType);
            } else {
                RexNode theta = joinInfo.getRemaining(join.getCluster().getRexBuilder());

                if (isSupported(theta)) {

                    if (join.getJoinType() == JoinRelType.INNER) {
                        // if it's inner then create an equi-join (maybe crossjoin) and then a filter.

                        RelNode left = convert(join.getLeft(), getOutTrait());
                        RelNode right = convert(join.getRight(), getOutTrait());

                        RelNode input = new SparqlEquiJoin(
                                join.getCluster(),
                                join.getTraitSet().replace(getOutTrait()),
                                left,
                                right,
                                joinInfo.getEquiCondition(left, right, join.getCluster().getRexBuilder()),
                                joinInfo.leftKeys,
                                joinInfo.rightKeys,
                                join.getVariablesSet(),
                                join.getJoinType());

                        return new SparqlFilter(
                                input.getCluster(),
                                input.getTraitSet().replace(getOutTrait()),
                                input,
                                theta);
                    } else {
                        assert join.getJoinType() == JoinRelType.LEFT;
                        try {
                            return new SparqlThetaJoin(
                                    join.getCluster(),
                                    join.getTraitSet().replace(out),
                                    convert(join.getLeft(), out),
                                    convert(join.getRight(), out),
                                    join.getCondition(),
                                    join.getVariablesSet(),
                                    join.getJoinType());
                        } catch (InvalidRelException e) {
                            LOGGER.debug(e.toString());
                            return null;
                        }
                    }
                }
            }
            return null;
        }
    }

    public static class SparqlEquiJoin extends EquiJoin implements SparqlRel {


        public SparqlEquiJoin(RelOptCluster cluster, RelTraitSet traits,
                              RelNode left, RelNode right, RexNode condition,
                              ImmutableIntList leftKeys, ImmutableIntList rightKeys,
                              Set<CorrelationId> variablesSet, JoinRelType joinType) {
            super(cluster, traits, left, right, condition, leftKeys, rightKeys, variablesSet, joinType);
        }

        @Override
        public Join copy(RelTraitSet traitSet, RexNode condition,
                         RelNode left, RelNode right, JoinRelType joinType,
                         boolean semiJoinDone) {


            final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
            assert joinInfo.isEqui();

            return new SparqlRules.SparqlEquiJoin(getCluster(), traitSet, left, right,
                    condition, joinInfo.leftKeys, joinInfo.rightKeys, variablesSet, joinType);

        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }
    }

    public static class SparqlThetaJoin extends Join implements SparqlRel {

        protected SparqlThetaJoin(RelOptCluster cluster, RelTraitSet traitSet,
                           RelNode left, RelNode right, RexNode condition,
                           Set<CorrelationId> variablesSet, JoinRelType joinType)
                throws InvalidRelException {
            super(cluster,
                    traitSet,
                    left,
                    right,
                    condition,
                    variablesSet,
                    joinType);
        }

        @Override
        public Join copy(RelTraitSet traitSet, RexNode condition,
            RelNode left, RelNode right, JoinRelType joinType,
            boolean semiJoinDone) {

            try {
                return new SparqlRules.SparqlThetaJoin(getCluster(), traitSet, left, right,
                        condition, variablesSet, joinType);
            } catch (InvalidRelException e) {
                // Semantic error not possible. Must be a bug. Convert to
                // internal error.
                throw new AssertionError(e);
            }
        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

    }

    private static class SparqlFilterRule extends SparqlConverterRule {

        public SparqlFilterRule(SparqlConvention out) {
            super(LogicalFilter.class, Convention.NONE, out, "SparqlFilterRule");
        }

        @Override
        public RelNode convert(RelNode relNode) {

            LogicalFilter filter = (LogicalFilter) relNode;

            if (!isSupported(filter.getCondition()))
                return null;


            return new SparqlRules.SparqlFilter(
                    filter.getCluster(),
                    filter.getTraitSet().replace(out),
                    convert(filter.getInput(), out),
                    filter.getCondition());
        }
    }

    public static class SparqlFilter extends Filter implements SparqlRel {

        protected SparqlFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
            super(cluster, traits, child, condition);
            assert getConvention() instanceof SparqlConvention;
        }

        @Override
        public Filter copy(RelTraitSet relTraitSet, RelNode relNode, RexNode rexNode) {
            return new SparqlFilter(getCluster(), relTraitSet, relNode, rexNode);
        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

    }

    private static class SparqlProjectRule extends SparqlConverterRule {

        public SparqlProjectRule(SparqlConvention out) {
            super(LogicalProject.class, Convention.NONE, out, "SparqlProjectRule");
        }

        @Override
        public RelNode convert(RelNode relNode) {

            LogicalProject project = (LogicalProject) relNode;

            for (RexNode p : project.getProjects()) {
                if (!isSupported(p))
                    return null;
            }

            // check condition (if can be transformed)
            return new SparqlProject(
                    project.getCluster(),
                    project.getTraitSet().replace(out),
                    convert(project.getInput(), out),
                    project.getProjects(),
                    project.getRowType());
        }
    }

    public static class SparqlProject extends Project implements SparqlRel {

        protected SparqlProject(RelOptCluster cluster,
                                RelTraitSet traits,
                                RelNode input,
                                List<? extends RexNode> projects,
                                RelDataType rowType) {
            super(cluster, traits, input, projects, rowType);
        }

        @Override
        public Project copy(RelTraitSet relTraitSet, RelNode relNode, List<RexNode> list, RelDataType relDataType) {
            return new SparqlProject(getCluster(), relTraitSet, relNode, list, relDataType);
        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

    }

    private static class SparqlSortRule extends SparqlConverterRule {

        public SparqlSortRule(SparqlConvention out) {
            super(LogicalSort.class, Convention.NONE, out, "SparqlSortRule");
        }

        @Override
        public RelNode convert(RelNode relNode) {

            LogicalSort sort = (LogicalSort) relNode;

            // check condition (if can be transformed)
            return new SparqlSort(
                    sort.getCluster(),
                    sort.getTraitSet().replace(out),
                    convert(sort.getInput(),out),
                    sort.getCollation(),
                    sort.offset,
                    sort.fetch);
        }
    }

    public static class SparqlSort extends Sort implements SparqlRel {


        public SparqlSort(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
            super(cluster, traits, child, collation, offset, fetch);
        }

        @Override
        public Sort copy(RelTraitSet relTraitSet, RelNode relNode, RelCollation relCollation, RexNode rexNode, RexNode rexNode1) {
            return new SparqlSort(getCluster(), relTraitSet, relNode, relCollation, rexNode, rexNode1);
        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

    }

    private static class SparqlUnionRule extends SparqlConverterRule {

        public SparqlUnionRule(SparqlConvention out) {
            super(LogicalUnion.class, Convention.NONE, out, "SparqlUnionRule");
        }

        @Override
        public RelNode convert(RelNode relNode) {

            LogicalUnion node = (LogicalUnion) relNode;

            return new SparqlUnion(
                    relNode.getCluster(),
                    relNode.getTraitSet().replace(getOutTrait()),
                    convertList(node.getInputs(), getOutTrait()),
                    node.all);
        }
    }

    public static class SparqlUnion extends Union implements SparqlRel {

        protected SparqlUnion(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
            super(cluster, traits, inputs, all);
        }

        @Override
        public SetOp copy(RelTraitSet relTraitSet, List<RelNode> list, boolean b) {
            return new SparqlUnion(getCluster(), relTraitSet, list, b);
        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

    }

    private static class SparqlIntersectRule extends SparqlConverterRule {

        public SparqlIntersectRule(SparqlConvention out) {
            super(LogicalIntersect.class, Convention.NONE, out, "SparqlIntersectRule");
        }

        @Override
        public RelNode convert(RelNode relNode) {
            LogicalIntersect node = (LogicalIntersect) relNode;

            return new SparqlIntersect(
                    relNode.getCluster(),
                    relNode.getTraitSet().replace(getOutTrait()),
                    convertList(node.getInputs(), getOutTrait()),
                    node.all);
        }
    }

    public static class SparqlIntersect extends Intersect implements SparqlRel {

        public SparqlIntersect(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
            super(cluster, traits, inputs, all);
        }

        @Override
        public SetOp copy(RelTraitSet relTraitSet, List<RelNode> list, boolean b) {
            return new SparqlIntersect(getCluster(), relTraitSet ,list, b);
        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

    }

    private static class SparqlMinusRule extends SparqlConverterRule {

        public SparqlMinusRule(SparqlConvention out) {
            super(LogicalMinus.class, Convention.NONE, out, "SparqlMinusRule");
        }

        @Override
        public RelNode convert(RelNode relNode) {
            LogicalMinus node = (LogicalMinus) relNode;
            return new SparqlMinus(
                    relNode.getCluster(),
                    relNode.getTraitSet().replace(getOutTrait()),
                    convertList(node.getInputs(), getOutTrait()),
                    node.all);
        }
    }

    public static class SparqlMinus extends Minus implements SparqlRel {

        public SparqlMinus(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
            super(cluster, traits, inputs, all);
        }

        @Override
        public SetOp copy(RelTraitSet relTraitSet, List<RelNode> list, boolean b) {
            return new SparqlMinus(getCluster(), relTraitSet, list, b);
        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

    }

    private static class SparqlAggregateRule extends SparqlConverterRule {

        public SparqlAggregateRule(SparqlConvention out) {
            super(LogicalAggregate.class, Convention.NONE, out, "SparqlAggregateRule");
        }

        @Override
        public RelNode convert(RelNode relNode) {
            LogicalAggregate node = (LogicalAggregate) relNode;

            if (node.getAggCallList().isEmpty() &&
                isEveryColumnInGroupSet(node.getRowType(), node.getGroupSet())) {

                return new SparqlDistinct(
                        relNode.getCluster(),
                        relNode.getTraitSet().replace(getOutTrait()),
                        convert(node.getInput(), getOutTrait()));
            } else {

                if (out.getDialect().getVersion() == SparqlVersion.SPARQL10)
                    return null; //groupings are not supported in Sparql10 (only distinct)

                for (AggregateCall call : node.getAggCallList())
                    if (!isSupported(call.getAggregation()))
                        return null;

                return new SparqlAggregate(
                        relNode.getCluster(),
                        relNode.getTraitSet().replace(getOutTrait()),
                        convert(node.getInput(), getOutTrait()),
                        node.indicator,
                        node.getGroupSet(),
                        node.getGroupSets(),
                        node.getAggCallList());
            }
        }


        private boolean isEveryColumnInGroupSet(RelDataType rowType, ImmutableBitSet groupSet) {
            for (RelDataTypeField f : rowType.getFieldList()) {
                if (!groupSet.get(f.getIndex()))
                    return false;
            }
            return true;
        }
    }

    public static class SparqlDistinct extends Aggregate implements SparqlRel {

        protected SparqlDistinct(RelOptCluster cluster, RelTraitSet traits, RelNode child) {
            super(cluster,
                    traits,
                    child,
                    false,
                    ImmutableBitSet.range(child.getRowType().getFieldCount()),
                    null,
                    ImmutableList.of());
        }

        @Override
        public Aggregate copy(RelTraitSet traitSet,
                              RelNode input,
                              boolean indicator,
                              ImmutableBitSet groupSet,
                              List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
            return copy(traitSet, input);
        }

        public SparqlDistinct copy(RelTraitSet traitSet, RelNode input) {
            return new SparqlDistinct(getCluster(), traitSet, input);
        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

    }

    public static class SparqlAggregate extends Aggregate implements SparqlRel {

        protected SparqlAggregate(RelOptCluster cluster, RelTraitSet traits, RelNode child, boolean indicator,
                                  ImmutableBitSet groupSet, List<ImmutableBitSet> groupSets,
                                  List<AggregateCall> aggCalls) {
            super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
        }

        @Override
        public Aggregate copy(RelTraitSet relTraitSet, RelNode relNode, boolean b,
                              ImmutableBitSet immutableBitSet, List<ImmutableBitSet> list,
                              List<AggregateCall> list1) {
            return new SparqlAggregate(
                    getCluster(),
                    relTraitSet,
                    relNode,
                    b,
                    immutableBitSet,
                    list,
                    list1);
        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

    }

    private static class SparqlValuesRule extends SparqlConverterRule {

        public SparqlValuesRule(SparqlConvention out) {
            super(LogicalValues.class, Convention.NONE, out, "SparqlValuesRule");
        }

        @Override
        public RelNode convert(RelNode relNode) {

            LogicalValues node = (LogicalValues) relNode;

            return new SparqlValues(
                    relNode.getCluster(),
                    relNode.getRowType(),
                    node.getTuples(),
                    relNode.getTraitSet().replace(getOutTrait()));
        }
    }

    public static class SparqlValues extends Values implements SparqlRel {

        protected SparqlValues(
                RelOptCluster cluster,
                RelDataType rowType,
                ImmutableList<ImmutableList<RexLiteral>> tuples,
                RelTraitSet traits) {
            super(cluster, rowType, tuples, traits);
        }

        public SparqlImplementor.Result implement(SparqlImplementor implementor) { return implementor.implement(this); }

    }

}
