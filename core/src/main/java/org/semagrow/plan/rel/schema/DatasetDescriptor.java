package org.semagrow.plan.rel.schema;

import org.apache.calcite.plan.RelOptPlanner;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;


/**
 * A description of a dataset, namely a set of triples of various graphs.
 */
public interface DatasetDescriptor {

    GraphEntry getGraph();

    /**
     * Returns a segment of the dataset that satisfies a pattern.
     * */
    RelOptDataset getSegment(Resource subj, IRI pred, Value obj, Resource... graph);

    void registerRules(RelOptPlanner planner);

    interface GraphEntry {
        String getName();
    }

    interface Factory {
        DatasetDescriptor create(Resource config);
    }

    interface DomainConstraint {

    }
}
