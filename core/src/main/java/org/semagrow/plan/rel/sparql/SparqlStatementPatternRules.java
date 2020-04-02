package org.semagrow.plan.rel.sparql;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public  class SparqlStatementPatternRules {

    private static class SparqlFilterStatementScanRule extends RelOptRule {

        private SparqlFilterStatementScanRule(RelBuilderFactory builderFactory) {
            super(operand(Filter.class,
                    operand(SparqlStatementPattern.class, none())),
                    builderFactory, null);
        }

        // Utility function
        static public RexNode classifyFilters(RexBuilder builder,
                                              RexNode condition,
                                              List<RexNode> equal) {
            final List<RexNode> nonEqualList = new ArrayList<>();
            List<RexNode> conjuncts = RelOptUtil.conjunctions(condition);

            classifyFilters(conjuncts, equal, nonEqualList);

            return RexUtil.composeConjunction(builder, nonEqualList, false);
        }

        private static void classifyFilters(List<RexNode> conjuncts,
                                            List<RexNode> simpleEqual,
                                            List<RexNode> nonEqualList) {
            for (RexNode exp : conjuncts) {
                if (isSimpleEqual(exp)) {
                    simpleEqual.add(exp);
                } else {
                    nonEqualList.add(exp);
                }
            }
        }

        private static boolean isSimpleEqual(RexNode exp) {

            if (exp instanceof RexCall &&
                    exp.isA(SqlKind.EQUALS)) {
                RexCall eq = (RexCall) exp;
                RexNode left = eq.operands.get(0);
                RexNode right = eq.operands.get(1);

                RexNode other = null;

                if (left instanceof RexInputRef)
                    other = right;
                else if (right instanceof RexInputRef)
                    other = left;

                return other != null &&
                        ((other instanceof RexLiteral) || (other instanceof RexInputRef));
            }

            return false;
        }

        private static Map<Integer,RexNode> equalToMap(Iterable<? extends RexNode> nodes) {
            ImmutableMap.Builder<Integer,RexNode> out = ImmutableMap.builder();

            for (RexNode n : nodes) {
                if (n instanceof RexCall && n.isA(SqlKind.EQUALS)) {
                    RexCall eq = (RexCall) n;
                    RexNode left = eq.operands.get(0);
                    RexNode right = eq.operands.get(1);

                    if (RexUtil.eq(left, right))
                        continue;

                    RexNode ref = (left instanceof RexInputRef) ? left : right;
                    RexNode other = (left instanceof RexInputRef) ? right : left;
                    if (ref instanceof RexInputRef) {
                        RexInputRef v = (RexInputRef)ref;
                        out.put(v.getIndex(), other);
                    }
                }
            }
            return out.build();
        }

        private static boolean mergeFilters(Map<Integer, RexNode> filter1,
                                            Map<Integer, RexNode> filter2,
                                            Map<Integer, RexNode> merged) {


            Map<Integer, RexNode> literals    = new HashMap<>();
            Map<Integer, RexNode> nonLiterals = new HashMap<>();


            for (Map.Entry<Integer, RexNode> e : filter1.entrySet()) {
                if (e.getValue() instanceof RexLiteral)
                    literals.put(e.getKey(), e.getValue());
            }

            for (Map.Entry<Integer, RexNode> e : filter2.entrySet()) {

                if (literals.containsKey(e.getKey()))
                    return false;

                if (e.getValue() instanceof RexLiteral)
                    literals.put(e.getKey(), e.getValue());
            }

            //FIXME: restructure this ugly code

            for (Map.Entry<Integer, RexNode> e : filter1.entrySet()) {
                if (e.getValue() instanceof RexInputRef) {
                    RexInputRef r = (RexInputRef) e.getValue();

                    if (literals.containsKey(e.getKey())) {
                        if (literals.containsKey(r.getIndex())) {
                            if (!RexUtil.eq(literals.get(r.getIndex()), literals.get(e.getKey())))
                                return false;
                        } else
                            literals.put(r.getIndex(), literals.get(e.getKey()));

                    } else if (literals.containsKey(r.getIndex())) {
                        literals.put(e.getKey(), literals.get(r.getIndex()));
                    } else {
                        nonLiterals.put(e.getKey(), e.getValue());
                    }
                }
            }

            for (Map.Entry<Integer, RexNode> e : filter2.entrySet()) {
                if (e.getValue() instanceof RexInputRef) {
                    RexInputRef r = (RexInputRef) e.getValue();

                    if (literals.containsKey(e.getKey())) {
                        if (literals.containsKey(r.getIndex())) {
                            if (!RexUtil.eq(literals.get(r.getIndex()), literals.get(e.getKey())))
                                return false;
                        } else
                            literals.put(r.getIndex(), literals.get(e.getKey()));

                    } else if (literals.containsKey(r.getIndex())) {
                        literals.put(e.getKey(), literals.get(r.getIndex()));
                    } else {
                        nonLiterals.put(e.getKey(), e.getValue());
                    }
                }
            }

            merged.putAll(literals);
            merged.putAll(nonLiterals); // check if there are equalities {x} = {y} && {y} = {x}

            return true;
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final Filter filter = call.rel(0);
            final SparqlStatementPattern pattern = call.rel(1);

            List<RexNode> equalities = new ArrayList<>();

            final Mappings.TargetMapping mapping = pattern.getMapping();

            RexNode cond = classifyFilters(
                    filter.getCluster().getRexBuilder(),
                    filter.getCondition(),
                    equalities);

            Iterable<RexNode> equalities2 = RexUtil.apply(mapping.inverse(), equalities);

            Map<Integer, RexNode> filters = equalToMap(equalities2);
            Map<Integer, RexNode> merged = new HashMap<>();

            boolean alwaysFalse = mergeFilters(filters, pattern.filters, merged);

            RelNode rel;

            if (!alwaysFalse) {
                rel = SparqlStatementPattern.create(
                        pattern.getCluster(),
                        pattern.getDataset(),
                        pattern.getTraitSet(),
                        pattern.getRowType(),
                        merged,
                        pattern.projects);
            } else
                rel = LogicalValues.createEmpty(pattern.getCluster(), pattern.getRowType());

            if (cond.isAlwaysTrue()) {
                // ignore filter
                RelBuilder builder = call.builder();
                builder.push(rel);
                rel = builder.filter(cond).build();
            }

            call.transformTo(rel);
        }
    }

    private static class SparqlProjectStatementScanRule extends RelOptRule {

        private SparqlProjectStatementScanRule() {
            super(operand(Project.class, null, p -> p.isMapping(),
                      operand(SparqlStatementPattern.class, none())));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {

            final Project project = call.rel(0);
            final SparqlStatementPattern pattern = call.rel(1);
            final Mappings.TargetMapping mapping = project.getMapping();

            if (mapping == null) {
                return;
            }

            List<Integer> projects = Mappings.apply((Mapping)mapping, pattern.projects);

            RelNode rel = SparqlStatementPattern.create(
                    pattern.getCluster(),
                    pattern.getDataset(),
                    pattern.getTraitSet(),
                    project.getRowType(),
                    pattern.filters,
                    projects);

            call.transformTo(rel);
        }

    }

}
