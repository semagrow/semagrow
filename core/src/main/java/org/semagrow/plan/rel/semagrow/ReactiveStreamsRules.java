package org.semagrow.plan.rel.semagrow;


import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;


public class ReactiveStreamsRules {

    static RelOptRule REACTIVESTREAMS_HASH_JOIN_RULE = new ReactiveStreamsHashJoinRule();
    static RelOptRule REACTIVESTREAMS_MERGE_JOIN_RULE = new ReactiveStreamsMergeJoinRule();
    //static RelOptRule REACTIVESTREAMS_NLJ_JOIN_RULE = new ReactiveStreamsNLJJoinRule();
    static RelOptRule REACTIVESTREAMS_PROJECT_RULE = new ReactiveStreamsProjectRule();
    static RelOptRule REACTIVESTREAMS_FILTER_RULE = new ReactiveStreamsFilterRule();
    static RelOptRule REACTIVESTREAMS_AGGREGATE_RULE = new ReactiveStreamsAggregateRule();
    static RelOptRule REACTIVESTREAMS_SORT_RULE = new ReactiveStreamsSortRule();
    static RelOptRule REACTIVESTREAMS_INTERSECT_RULE = new ReactiveStreamsIntersectRule();
    static RelOptRule REACTIVESTREAMS_UNION_RULE = new ReactiveStreamsUnionRule();
    static RelOptRule REACTIVESTREAMS_VALUES_RULE = new ReactiveStreamsValuesRule();
    static RelOptRule REACTIVESTREAMS_MINUS_RULE = new ReactiveStreamsMinusRule();

    public static List<RelOptRule> RULES =
            ImmutableList.of(
                    REACTIVESTREAMS_HASH_JOIN_RULE,
                    //REACTIVESTREAMS_MERGE_JOIN_RULE,
                    REACTIVESTREAMS_PROJECT_RULE,
                    REACTIVESTREAMS_FILTER_RULE,
                    REACTIVESTREAMS_AGGREGATE_RULE,
                    REACTIVESTREAMS_SORT_RULE,
                    REACTIVESTREAMS_UNION_RULE,
                    REACTIVESTREAMS_INTERSECT_RULE,
                    REACTIVESTREAMS_MINUS_RULE,
                    REACTIVESTREAMS_VALUES_RULE);

    public static abstract class ReactiveStreamsConverterRule extends ConverterRule {

        public ReactiveStreamsConverterRule(Class<? extends RelNode> clazz, RelTrait in, RelTrait out, String description) {
            super(clazz, in, out, description);
        }

        public <R extends RelNode> ReactiveStreamsConverterRule(Class<R> clazz, Predicate<? super R> predicate, RelTrait in, RelTrait out, String description) {
            super(clazz, predicate, in, out, description);
        }
    }

    public static class ReactiveStreamsHashJoinRule extends ReactiveStreamsConverterRule {

        public ReactiveStreamsHashJoinRule() {
            super(LogicalJoin.class, Convention.NONE, ReactiveStreamsConvention.INSTANCE, "ReactiveStreamsHashJoinRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalJoin join = (LogicalJoin)rel;

            final RelNode left = convert(join.getLeft(), getOutTrait());
            final RelNode right = convert(join.getRight(), getOutTrait());
            final JoinInfo info = JoinInfo.of(left, right, join.getCondition());

            if (!info.isEqui() && join.getJoinType() != JoinRelType.INNER) {

                return new ReactiveStreamsThetaJoin(join.getCluster(),
                        join.getTraitSet().replace(getOutConvention()),
                        left,
                        right,
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType());
            }

            RelNode newRel = new ReactiveStreamsHashJoin(
                    join.getCluster(),
                    join.getTraitSet().replace(getOutConvention()),
                    left,
                    right,
                    info.getEquiCondition(left,right,join.getCluster().getRexBuilder()),
                    info.leftKeys,
                    info.rightKeys,
                    join.getVariablesSet(),
                    join.getJoinType());

            if (!info.isEqui()) {
                newRel = new ReactiveStreamsFilter(join.getCluster(), newRel.getTraitSet(),
                        newRel, info.getRemaining(join.getCluster().getRexBuilder()));
            }
            return newRel;
        }
    }

    public static class ReactiveStreamsThetaJoin extends Join implements ReactiveStreamsRel {

        protected ReactiveStreamsThetaJoin(RelOptCluster cluster,
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
        public ReactiveStreamsThetaJoin copy(RelTraitSet traitSet,
                                             RexNode conditionExpr,
                                             RelNode left,
                                             RelNode right,
                                             JoinRelType joinType,
                                             boolean semiJoinDone)
        {
            return new ReactiveStreamsThetaJoin(getCluster(),
                    traitSet,
                    left,
                    right,
                    conditionExpr,
                    ImmutableSet.of(),
                    joinType);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {
            final Bindable<Object[]> left = ((ReactiveStreamsRel) getLeft()).implement(implementor);
            final Bindable<Object[]> right = ((ReactiveStreamsRel) getLeft()).implement(implementor);

            return new Bindable<Object[]>() {
                @Override
                public Publisher<Object[]> bind(DataContext<Object[]> ctx) {
                    Publisher<Object[]> leftPub = left.bind(ctx);
                    Publisher<Object[]> rightPub =right.bind(ctx);
                    return ReactorImpl.hashJoin(
                            ReactorImpl.asFlux(leftPub),
                            ReactorImpl.asFlux(rightPub),
                            null,
                            null,
                            null,
                            joinType.generatesNullsOnLeft(),
                            joinType.generatesNullsOnRight());
                }
            };
        }
    }

    public static class ReactiveStreamsHashJoin extends EquiJoin implements ReactiveStreamsRel {

        protected ReactiveStreamsHashJoin(RelOptCluster cluster,
                               RelTraitSet traitSet,
                               RelNode left,
                               RelNode right,
                               RexNode condition,
                               ImmutableIntList leftKeys,
                               ImmutableIntList rightKeys,
                               Set<CorrelationId> variablesSet,
                               JoinRelType joinType)
        {
            super(cluster, traitSet, left, right, condition, leftKeys, rightKeys, variablesSet, joinType);
        }

        @Override
        public ReactiveStreamsHashJoin copy(RelTraitSet traitSet, RexNode condition,
                                            RelNode left, RelNode right, JoinRelType joinType,
                                            boolean semiJoinDone)
        {
            final JoinInfo joinInfo = JoinInfo.of(left, right, condition);
            assert joinInfo.isEqui();

            return new ReactiveStreamsHashJoin(getCluster(),
                    traitSet,
                    left,
                    right,
                    condition,
                    joinInfo.leftKeys,
                    joinInfo.rightKeys,
                    variablesSet,
                    joinType);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {
            final Bindable<Object[]> left = ((ReactiveStreamsRel) getLeft()).implement(implementor);
            final Bindable<Object[]> right = ((ReactiveStreamsRel) getLeft()).implement(implementor);

            final Function<Object[], Object[]> outerKeySelector = TypeUtil.getAccessor(leftKeys);
            final Function<Object[], Object[]> innerKeySelector = TypeUtil.getAccessor(rightKeys);
            final BiFunction<Object[], Object[], Object[]> resultKeySelector = TypeUtil.getJoinSelector(rowType, getLeft().getRowType(), getRight().getRowType());

            return new Bindable<Object[]>() {
                @Override
                public Publisher<Object[]> bind(DataContext<Object[]> ctx) {
                    Publisher<Object[]> leftPub = left.bind(ctx);
                    Publisher<Object[]> rightPub =right.bind(ctx);
                    return ReactorImpl.hashJoin(
                            ReactorImpl.asFlux(leftPub),
                            ReactorImpl.asFlux(rightPub),
                            outerKeySelector,
                            innerKeySelector,
                            resultKeySelector,
                            joinType.generatesNullsOnLeft(),
                            joinType.generatesNullsOnRight());
                }
            };
        }
    }

    public static class ReactiveStreamsMergeJoinRule extends ReactiveStreamsConverterRule {

        public ReactiveStreamsMergeJoinRule() {
            super(LogicalJoin.class, Convention.NONE,
                    ReactiveStreamsConvention.INSTANCE,
                    "ReactiveStreamsMergeJoinRule");
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

            newRel = new ReactiveStreamsMergeJoin(join.getCluster(),
                    join.getTraitSet().replace(getOutConvention()),
                    convert(join.getLeft(), getOutTrait()),
                    convert(join.getRight(), getOutTrait()),
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType());


            if (!info.isEqui()) {
                newRel = new ReactiveStreamsFilter(cluster, newRel.getTraitSet(),
                        newRel, info.getRemaining(cluster.getRexBuilder()));
            }
            return newRel;

        }
    }

    public static class ReactiveStreamsMergeJoin extends Join implements ReactiveStreamsRel {

        protected ReactiveStreamsMergeJoin(RelOptCluster cluster,
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
        public ReactiveStreamsMergeJoin copy(RelTraitSet traitSet,
                                                             RexNode conditionExpr,
                                                             RelNode left,
                                                             RelNode right,
                                                             JoinRelType joinType,
                                                             boolean semiJoinDone)
        {
            return new ReactiveStreamsMergeJoin(getCluster(),
                    traitSet,
                    left,
                    right,
                    conditionExpr,
                    ImmutableSet.of(),
                    joinType);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {
            throw new NotImplementedException();
        }
    }

    public static class ReactiveStreamsProjectRule extends ReactiveStreamsConverterRule {

        ReactiveStreamsProjectRule() {
            super(LogicalProject.class, Convention.NONE, ReactiveStreamsConvention.INSTANCE, "ReactiveStreamsProjectRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalProject project = (LogicalProject) rel;
            return new ReactiveStreamsRules.ReactiveStreamsProject(project.getCluster(),
                    project.getTraitSet().replace(getOutConvention()),
                    convert(project.getInput(), getOutTrait()),
                    project.getProjects(),
                    project.getRowType());
        }
    }

    public static class ReactiveStreamsProject extends Project implements ReactiveStreamsRel {

        protected ReactiveStreamsProject(RelOptCluster cluster,
                                  RelTraitSet traits,
                                  RelNode input,
                                  List<? extends RexNode> projects, RelDataType rowType) {
            super(cluster, traits, input, projects, rowType);
        }

        @Override
        public ReactiveStreamsProject copy(RelTraitSet traitSet, RelNode input, List<RexNode> projects, RelDataType rowType) {
            return new ReactiveStreamsProject(getCluster(), traitSet, input, projects, rowType);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {
            throw new NotImplementedException();
        }
    }

    public static class ReactiveStreamsFilterRule extends ReactiveStreamsConverterRule {

        ReactiveStreamsFilterRule() {
            super(LogicalFilter.class, Convention.NONE, ReactiveStreamsConvention.INSTANCE, "ReactiveStreamsFilterRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalFilter filter = (LogicalFilter)rel;
            return new ReactiveStreamsFilter(filter.getCluster(),
                    filter.getTraitSet().replace(getOutConvention()),
                    convert(filter.getInput(), getOutTrait()),
                    filter.getCondition()
            );
        }
    }

    public static class ReactiveStreamsFilter extends Filter implements ReactiveStreamsRel {

        protected ReactiveStreamsFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
            super(cluster, traits, child, condition);
        }

        @Override
        public ReactiveStreamsFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
            return new ReactiveStreamsFilter(getCluster(), traitSet, input, condition);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {

            final Bindable<Object[]> input = ((ReactiveStreamsRel)getInput()).implement(implementor);

            return new Bindable<Object[]>() {
                @Override
                public Publisher<Object[]> bind(DataContext<Object[]> ctx) {
                    Publisher<Object[]> source = input.bind(ctx);
                    java.util.function.Predicate<Object[]> p = new java.util.function.Predicate<Object[]>() {
                        @Override
                        public boolean test(Object[] objects) {
                            return false;
                        }
                    };
                    return ReactorImpl.filter(ReactorImpl.asFlux(source), p);
                }
            };
        }
    }

    public static class ReactiveStreamsAggregateRule extends ReactiveStreamsConverterRule {

        ReactiveStreamsAggregateRule(){
            super(LogicalAggregate.class, Convention.NONE, ReactiveStreamsConvention.INSTANCE, "ReactiveStreamsAggregateRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalAggregate aggregate = (LogicalAggregate) rel;
            return new ReactiveStreamsAggregate(aggregate.getCluster(),
                    aggregate.getTraitSet().replace(getOutConvention()),
                    convert(aggregate.getInput(), getOutTrait()),
                    aggregate.indicator,
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    aggregate.getAggCallList());
        }
    }

    public static class ReactiveStreamsAggregate extends Aggregate implements ReactiveStreamsRel {

        protected ReactiveStreamsAggregate(RelOptCluster cluster,
                                    RelTraitSet traits,
                                    RelNode child,
                                    boolean indicator,
                                    ImmutableBitSet groupSet,
                                    List<ImmutableBitSet> groupSets,
                                    List<AggregateCall> aggCalls) {
            super(cluster, traits, child, indicator, groupSet, groupSets, aggCalls);
        }

        @Override
        public ReactiveStreamsAggregate copy(RelTraitSet traitSet,
                                            RelNode input,
                                            boolean indicator,
                                            ImmutableBitSet groupSet,
                                            List<ImmutableBitSet> groupSets,
                                            List<AggregateCall> aggCalls) {
            return new ReactiveStreamsAggregate(getCluster(), traitSet, input, indicator, groupSet, groupSets, aggCalls);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {
            throw new NotImplementedException();
        }
    }

    public static class ReactiveStreamsSortRule extends ReactiveStreamsConverterRule {


        ReactiveStreamsSortRule() {
            super(LogicalSort.class, Convention.NONE, ReactiveStreamsConvention.INSTANCE, "ReactiveStreamsSortRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalSort sort = (LogicalSort) rel;
            return new ReactiveStreamsSort(sort.getCluster(),
                    sort.getTraitSet().replace(getOutConvention()),
                    convert(sort.getInput(), getOutTrait()),
                    sort.getCollation(),
                    sort.offset,
                    sort.fetch);
        }
    }

    public static class ReactiveStreamsSort extends Sort implements ReactiveStreamsRel {

        public ReactiveStreamsSort(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
            super(cluster, traits, child, collation, offset, fetch);
        }

        @Override
        public ReactiveStreamsRules.ReactiveStreamsSort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
            return new ReactiveStreamsRules.ReactiveStreamsSort(getCluster(), traitSet, newInput, newCollation, offset, fetch);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {
            Bindable<Object[]> input = ((ReactiveStreamsRel) getInput()).implement(implementor);

            if (offset != null) {
                int skip = RexLiteral.intValue(offset);
            }

            if (fetch != null) {
                int limit = RexLiteral.intValue(fetch);
            }

            throw new NotImplementedException();
        }
    }

    public static class ReactiveStreamsIntersectRule extends ReactiveStreamsConverterRule {

        ReactiveStreamsIntersectRule() {
            super(LogicalIntersect.class, Convention.NONE, ReactiveStreamsConvention.INSTANCE, "ReactiveStreamsIntersectRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalIntersect intersect = (LogicalIntersect) rel;
            return new ReactiveStreamsIntersect(
                    intersect.getCluster(),
                    intersect.getTraitSet().replace(getOutConvention()),
                    convertList(intersect.getInputs(), getOutTrait()),
                    intersect.all);
        }
    }

    public static class ReactiveStreamsIntersect extends Intersect implements ReactiveStreamsRel {

        public ReactiveStreamsIntersect(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
            super(cluster, traits, inputs, all);
        }

        @Override
        public ReactiveStreamsIntersect copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
            return new ReactiveStreamsIntersect(getCluster(), traitSet, inputs, all);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {
            List<RelNode> inputs = this.getInputs();
            final List<Bindable<Object[]>> bindables = new ArrayList<>(inputs.size());

            for (RelNode node : inputs) {
                ReactiveStreamsRel rel = (ReactiveStreamsRel) node;
                bindables.add(rel.implement(implementor));
            }

            return new Bindable<Object[]>() {
                @Override
                public Publisher<Object[]> bind(DataContext<Object[]> ctx) {

                    return Flux.fromIterable(bindables)
                            .map(x -> x.bind(ctx))
                            .map(ReactorImpl::asFlux)
                            .reduce(ReactorImpl::intersect)
                            .flatMap(x -> x);
                }
            };
        }
    }

    public static class ReactiveStreamsUnionRule extends ReactiveStreamsConverterRule {

        ReactiveStreamsUnionRule() {
            super(LogicalUnion.class, Convention.NONE, ReactiveStreamsConvention.INSTANCE, "ReactiveStreamsUnionRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalUnion intersect = (LogicalUnion) rel;
            return new ReactiveStreamsUnion(
                    intersect.getCluster(),
                    intersect.getTraitSet().replace(getOutConvention()),
                    convertList(intersect.getInputs(), getOutTrait()),
                    intersect.all);
        }
    }

    public static class ReactiveStreamsUnion extends Union implements ReactiveStreamsRel {

        protected ReactiveStreamsUnion(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
            super(cluster, traits, inputs, all);
        }

        @Override
        public ReactiveStreamsUnion copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
            return new ReactiveStreamsRules.ReactiveStreamsUnion(getCluster(), traitSet, inputs, all);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {
            List<RelNode> inputs = this.getInputs();
            final List<Bindable<Object[]>> bindables = new ArrayList<>(inputs.size());

            for (RelNode node : inputs) {
                ReactiveStreamsRel rel = (ReactiveStreamsRel) node;
                bindables.add(rel.implement(implementor));
            }

            return new Bindable<Object[]>() {
                @Override
                public Publisher<Object[]> bind(DataContext<Object[]> ctx) {

                    return Flux.fromIterable(bindables)
                            .map(x -> x.bind(ctx))
                            .map(ReactorImpl::asFlux)
                            .reduce(ReactorImpl::union)
                            .flatMap(x -> x);
                }
            };
        }
    }

    public static class ReactiveStreamsMinusRule extends ReactiveStreamsConverterRule {

        ReactiveStreamsMinusRule() {
            super(LogicalMinus.class, Convention.NONE, ReactiveStreamsConvention.INSTANCE, "ReactiveStreamsMinusRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalMinus intersect = (LogicalMinus) rel;
            return new ReactiveStreamsMinus(
                    intersect.getCluster(),
                    intersect.getTraitSet().replace(getOutConvention()),
                    convertList(intersect.getInputs(), getOutTrait()),
                    intersect.all);
        }
    }

    public static class ReactiveStreamsMinus extends Minus implements ReactiveStreamsRel {

        public ReactiveStreamsMinus(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
            super(cluster, traits, inputs, all);
        }

        @Override
        public ReactiveStreamsMinus copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
            return new ReactiveStreamsRules.ReactiveStreamsMinus(getCluster(),
                    traitSet,
                    inputs,
                    all);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {
            List<RelNode> inputs = this.getInputs();
            final List<Bindable<Object[]>> bindables = new ArrayList<>(inputs.size());

            for (RelNode node : inputs) {
                ReactiveStreamsRel rel = (ReactiveStreamsRel) node;
                bindables.add(rel.implement(implementor));
            }

            return new Bindable<Object[]>() {
                @Override
                public Publisher<Object[]> bind(DataContext<Object[]> ctx) {

                    return Flux.fromIterable(bindables)
                            .map(x -> x.bind(ctx))
                            .map(ReactorImpl::asFlux)
                            .reduce(ReactorImpl::except)
                            .flatMap(x -> x);
                }
            };
        }
    }

    public static class ReactiveStreamsValuesRule extends ReactiveStreamsConverterRule {
        ReactiveStreamsValuesRule() {
            super(LogicalValues.class, Convention.NONE, ReactiveStreamsConvention.INSTANCE, "ReactiveStreamsValuesRule");
        }

        @Override
        public RelNode convert(RelNode rel) {
            LogicalValues values = (LogicalValues)rel;
            return new ReactiveStreamsValues(values.getCluster(),
                    values.getRowType(),
                    values.tuples,
                    values.getTraitSet().replace(getOutConvention()));
        }
    }

    public static class ReactiveStreamsValues extends Values implements ReactiveStreamsRel {

        protected ReactiveStreamsValues(RelOptCluster cluster,
                                 RelDataType rowType,
                                 ImmutableList<ImmutableList<RexLiteral>> tuples,
                                 RelTraitSet traits) {
            super(cluster, rowType, tuples, traits);
        }

        @Override
        public Bindable<Object[]> implement(ReactiveStreamsImplementor implementor) {
            throw new NotImplementedException();
        }
    }



}
