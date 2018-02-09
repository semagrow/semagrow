package org.semagrow.plan.rel.sparql;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.Mappings;
import org.semagrow.plan.rel.StatementScan;
import org.semagrow.plan.rel.schema.RelOptDataset;

import java.util.List;
import java.util.Map;

public class SparqlStatementPattern extends StatementScan implements SparqlRel {

    private RelOptDataset dataset;

    private RelDataType rowType;

    public ImmutableIntList projects;

    public Map<Integer,RexNode> filters;

    protected SparqlStatementPattern(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptDataset dataset,
            RelDataType type,
            Map<Integer, RexNode> filters,
            List<Integer> projects)
    {
        super(cluster, traitSet, dataset);
        this.dataset = dataset;
        this.rowType = type;
        this.filters = ImmutableMap.copyOf(filters);
        this.projects = ImmutableIntList.copyOf(projects);
    }

    @Override protected RelDataType deriveRowType() {

        return dataset.getRowType(getCluster().getTypeFactory());
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }


    @Override
    public SparqlImplementor.Result implement(SparqlImplementor implementor) {
        return implementor.implement(this);
    }


    public Mappings.TargetMapping getMapping() {
        return Mappings.target(projects,
                dataset.getRowType(getCluster().getTypeFactory()).getFieldCount());
    }

    static public SparqlStatementPattern create(RelOptCluster cluster,
                                              RelOptDataset dataset,
                                              RelTraitSet traitSet,
                                              RelDataType rowType,
                                              Map<Integer, RexNode> filters,
                                              List<Integer> projects)
    {
        if (projects == null)
            projects = Mappings.asList(Mappings.createIdentity(rowType.getFieldCount()));

        return new SparqlStatementPattern(cluster,
                traitSet,
                dataset,
                rowType,
                filters,
                projects);
    }

    static public SparqlStatementPattern create(RelOptCluster cluster,
                                                RelOptDataset dataset,
                                                RelTraitSet traitSet)
    {
        RelDataType rowType = dataset.getRowType(cluster.getTypeFactory());

        return create(cluster, dataset,
                traitSet,
                rowType,
                ImmutableMap.of(),
                Mappings.asList(Mappings.createIdentity(rowType.getFieldCount())));
    }

}
