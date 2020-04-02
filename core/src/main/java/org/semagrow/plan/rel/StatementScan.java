package org.semagrow.plan.rel;

import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.semagrow.plan.rel.schema.RelOptDataset;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by angel on 2/7/2017.
 */
public abstract class StatementScan extends AbstractRelNode {

    private RelOptDataset dataset;

    protected StatementScan(RelOptCluster cluster,
                            RelTraitSet traitSet,
                            RelOptDataset dataset)
    {
        super(cluster, traitSet);
        this.dataset = Preconditions.checkNotNull(dataset);
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
        return dataset.getRowCount();
    }

    @Override protected RelDataType deriveRowType() {
        return dataset.getRowType(getCluster().getTypeFactory());
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .item("dataset", dataset.toString());
    }

    public RelOptDataset getDataset() { return dataset; }

    public RelNode projectAndFilter(ImmutableBitSet fieldsUsed,
                           List<String> nameList,
                           RexNode condition,
                           RelBuilder relBuilder) {

        final List<RexNode> exprList = new ArrayList<>();
        final RexBuilder rexBuilder = getCluster().getRexBuilder();

        assert fieldsUsed.cardinality() == nameList.size();

        for (int i : fieldsUsed) {
            exprList.add(rexBuilder.makeInputRef(this, i));
        }

        return relBuilder
                .push(this)
                .filter(condition)
                .project(exprList, nameList)
                .build();
    }
}