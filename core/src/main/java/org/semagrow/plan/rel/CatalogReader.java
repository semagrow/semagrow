package org.semagrow.plan.rel;


import org.semagrow.plan.rel.schema.DatasetDescriptor;
import org.semagrow.plan.rel.schema.RelOptDataset;
import org.semagrow.plan.rel.sparql.SparqlDatasetDescriptor;
import org.semagrow.plan.rel.sparql.SparqlDialect;

/**
 * A catalog reader contains the metadata needed for the datasets Semagrow federates.
 */
public interface CatalogReader {

    static CatalogReader INSTANCE = new Impl();

    RelOptDataset getDataset();

    class Impl implements CatalogReader {

        private DatasetDescriptor descriptor = new SparqlDatasetDescriptor(SparqlDialect.create(), "local");

        public RelOptDataset getDataset() { return descriptor.getSegment(null, null, null, null); }

    }
}
