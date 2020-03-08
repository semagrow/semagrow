package org.semagrow.geospatial.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.semagrow.model.vocabulary.SEVOD;

public final class SEVOD_GEO {

    public static final IRI DATASET_BOUNDING_BOX;

    static {
        ValueFactory vf = SimpleValueFactory.getInstance();
        DATASET_BOUNDING_BOX = vf.createIRI(SEVOD.NAMESPACE + "datasetBoundingBox");
    }
}
