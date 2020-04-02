package org.semagrow.plan.rel.semagrow;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.eclipse.rdf4j.query.algebra.QueryRoot;
import org.semagrow.plan.rel.CatalogReader;
import org.semagrow.plan.rel.RdfRexBuilder;
import org.semagrow.plan.rel.TupleExprToRelConverter;
import org.semagrow.plan.rel.type.RdfDataTypeFactory;


import java.util.List;

/**
 * Created by angel on 18/7/2017.
 */
public class SemagrowPrepare {

    private static final List<RelOptRule> DEFAULT_RULES =
            ImmutableList.of(
                    AggregateStarTableRule.INSTANCE,
                    AggregateStarTableRule.INSTANCE2,
                    TableScanRule.INSTANCE,
                    JoinAssociateRule.INSTANCE,
                    ProjectMergeRule.INSTANCE,
                    FilterTableScanRule.INSTANCE,
                    ProjectFilterTransposeRule.INSTANCE,
                    FilterProjectTransposeRule.INSTANCE,
                    FilterJoinRule.FILTER_ON_JOIN,
                    JoinPushExpressionsRule.INSTANCE,
                    AggregateExpandDistinctAggregatesRule.INSTANCE,
                    AggregateReduceFunctionsRule.INSTANCE,
                    FilterAggregateTransposeRule.INSTANCE,
                    ProjectWindowTransposeRule.INSTANCE,
                    JoinCommuteRule.INSTANCE,
                    JoinPushThroughJoinRule.RIGHT,
                    JoinPushThroughJoinRule.LEFT,
                    SortProjectTransposeRule.INSTANCE,
                    SortJoinTransposeRule.INSTANCE,
                    SortUnionTransposeRule.INSTANCE);

    public SemagrowPrepare() {
    }

    protected QueryParser createParser(String query) {
        return null; //new SparqlParser(query);
    }


    protected RelOptCluster createCluster(RelOptPlanner planner, RexBuilder rexBuilder) {
        return RelOptCluster.create(planner, rexBuilder);
    }

    protected RelOptPlanner createPlanner(
            Context context,
            org.apache.calcite.plan.Context externalContext,
            RelOptCostFactory costFactory) {

        final VolcanoPlanner planner = new VolcanoPlanner(costFactory, externalContext);

        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

        planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);

        // add abstract rules
        planner.registerAbstractRelationalRules();
        RelOptUtil.registerAbstractRels(planner);

        registerRules(planner, DEFAULT_RULES);

        // add implementation rules for convention SemagrowRel.CONVENTION

        return planner;
    }

    private static void registerRules(RelOptPlanner planner, List<RelOptRule> rules) {
        for (RelOptRule rule : rules)
            planner.addRule(rule);
    }


    protected Prepare.PreparedResult prepare(Context context, String query) {
        final RelOptPlanner planner = createPlanner(context, null, null);
        return prepare_(context, query, planner);
    }

    protected Prepare.PreparedResult prepare_(Context context, String query, RelOptPlanner planner) {

        final Convention resultConvention = getResultConvention();

        final Prepare prepare = new PrepareImpl(context, planner, resultConvention);
        final QueryParser parser = createParser(query);

        QueryRoot parsed = parser.parseQuery();

        return prepare.prepare(parsed);

    }

    protected Convention getResultConvention() { return SemagrowConvention.INSTANCE; }

    interface Context {
        // Config config();
        // CatalogReader getCatalogReader();
        RdfDataTypeFactory getTypeFactory();
        RelMetadataProvider getMetadataProvider();
    }


    interface QueryParser {
        String getQueryString();
        QueryRoot parseQuery();
    }

    /*
    class SparqlParser implements QueryParser {
        public SparqlParser(String query) { }
        TupleExpr parseQuery() {}
    }*/

    class PrepareImpl extends Prepare {

        private final Context context;
        private final RelOptPlanner planner;
        private final RdfRexBuilder rexBuilder;

        private PrepareImpl(
                Context context,
                RelOptPlanner planner,
                Convention resultConvention) {
            super(resultConvention);
            this.context = context;
            this.rexBuilder = new RdfRexBuilder(context.getTypeFactory());
            this.planner = planner;
        }

        @Override
        protected TupleExprToRelConverter getTupleExprToRelConverter() {
            final RelOptCluster cluster = createCluster(planner, rexBuilder);
            return new TupleExprToRelConverter(cluster, new CatalogReader.Impl());
        }

        @Override
        protected Program getProgram() {
            return Programs.standard(context.getMetadataProvider());
        }
    }

}
