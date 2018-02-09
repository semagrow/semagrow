package org.semagrow.plan.rel.schema;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class RelOptDatasetImpl implements RelOptDataset {

    private DatasetDescriptor datasetDescriptor;
    private Dataset dataset;

    RelOptDatasetImpl(DatasetDescriptor datasetDescriptor,
                      Dataset dataset)
    {
        //this.datasetDescriptor = Preconditions.checkNotNull(datasetDescriptor);
        this.dataset = dataset;
    }

    public static RelOptDataset create(DatasetDescriptor descriptor, Dataset dataset) {
        return new RelOptDatasetImpl(descriptor, dataset);
    }

    @Override
    public DatasetDescriptor getDatasetDescriptor() {
        return datasetDescriptor;
    }

    @Override
    public List<DatasetDescriptor.GraphEntry> getGraphs() {
        return ImmutableList.of();
    }

    @Override
    public RelNode toRel(ToRelContext context) {
        return dataset.toRel(context, this);
    }

    @Override
    public List<RelCollation> getCollationList() {
        return ImmutableList.of();
    }

    @Override
    public RelDistribution getDistribution() {
        RelDistribution dist = dataset.getStatistic().getDistribution();
        return (dist != null) ? dist : RelDistributionTraitDef.INSTANCE.getDefault();
    }

    @Override
    public List<DatasetDescriptor.DomainConstraint> getDomainConstraints() {
        return dataset.getStatistic().getConstraints();
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {
        return dataset.getRowType(factory);
    }

    @Override
    public Double getRowCount() {
        return dataset.getStatistic().getStatementCount();
    }

    @Override
    public boolean isKey(ImmutableBitSet columns) {
        return false;
    }

}
