package org.semagrow.plan.rel.schema;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public abstract class AbstractDataset implements Dataset {

    @Override
    public Statistic getStatistic() {
        return Statistics.UNKNOWN;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory factory) {

        final RelDataTypeFactory.FieldInfoBuilder type = factory.builder();

        for (Statement.Column col : Statement.Column.values())
            type.add(col.getName(), SqlTypeName.ANY);

        return type.build();
    }


    static class Statistics {
        public static Statistic UNKNOWN = new Statistic() {
            @Override
            public Double getStatementCount() {
                return 100d;
            }

            @Override
            public Double getSelectivity(RexNode node) {
                return RelMdUtil.guessSelectivity(node);
            }

            @Override
            public RelDistribution getDistribution() {
                return RelDistributionTraitDef.INSTANCE.getDefault();
            }

            @Override
            public List<DatasetDescriptor.DomainConstraint> getConstraints() {
                return ImmutableList.of();
            }

            @Override
            public boolean isKey(ImmutableBitSet columns) {
                return false;
            }
        };
    }
}
