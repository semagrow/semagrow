package org.semagrow.plan.rel.schema;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * A filterable segment of a graph database
 * A table of four elements (graph, subject, predicate, object)
 * A basic nature of a fragment is characterized by the simplicity
 * of the filters that can be applied.
 * A projection on a basic fragment is a triple pattern.
 * */
public interface RelOptDataset {

    DatasetDescriptor getDatasetDescriptor();

    List<DatasetDescriptor.GraphEntry> getGraphs();

    RelNode toRel(ToRelContext context);

    List<RelCollation> getCollationList();

    RelDistribution getDistribution();

    List<DatasetDescriptor.DomainConstraint> getDomainConstraints();

    RelDataType getRowType(RelDataTypeFactory factory);

    Double getRowCount();

    // distinct count.

    boolean isKey(ImmutableBitSet columns);

    interface ToRelContext {
        RelOptCluster getCluster();
    }
}
