package org.semagrow.plan.rel.schema;

import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

// a set of statements (s,p,o,g)

/***
 * A {@code Dataset} is a set of statements (i.e. quads of (s,p,o,g))
 * that can be accessed in a particular way.
 */
public interface Dataset {

    Statistic getStatistic();

    RelDataType getRowType(RelDataTypeFactory factory);

    RelNode toRel(
            RelOptDataset.ToRelContext context,
            RelOptDataset dataset
    );

    interface Statistic {

        Double getStatementCount();

        Double getSelectivity(RexNode node);

        RelDistribution getDistribution();

        List<DatasetDescriptor.DomainConstraint> getConstraints();

        boolean isKey(ImmutableBitSet columns);

    }

    interface Statement {

        enum Column {

            SUBJECT("s"),
            PREDICATE("p"),
            OBJECT("o"),
            GRAPH("g");

            private String name;

            Column(String columnName)  {
                this.name = columnName;
            }

            public String getName() { return name; }
        }

    }
}
