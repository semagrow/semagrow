package org.semagrow.plan.rel.sparql;

import org.apache.calcite.plan.RelOptPlanner;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.semagrow.plan.rel.schema.DatasetDescriptor;
import org.semagrow.plan.rel.schema.RelOptDataset;
import org.semagrow.plan.rel.schema.RelOptDatasetImpl;

public class SparqlDatasetDescriptor implements DatasetDescriptor {

    private final SparqlDialect dialect;
    private final String endpoint;

    private SparqlConvention convention;

    public SparqlDatasetDescriptor(SparqlDialect dialect, String endpoint) {
        this.dialect = dialect;
        this.endpoint = endpoint;
        this.convention = SparqlConvention.of(dialect, endpoint);
    }

    @Override
    public GraphEntry getGraph() {
        return null;
    }

    @Override
    public RelOptDataset getSegment(Resource subj, IRI pred, Value obj, Resource... graph) {
        SparqlDataset dataset = new SparqlDataset(convention);
        return RelOptDatasetImpl.create(this, dataset);
    }

    @Override
    public void registerRules(RelOptPlanner planner) { }
}
