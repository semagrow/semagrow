package org.semagrow.geospatial.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public final class UOM {
    public static final String NAMESPACE = "http://www.opengis.net/def/uom/OGC/1.0/";

    public static final String PREFIX = "uom";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public final static IRI degree;
    public final static IRI GridSpacing;
    public final static IRI metre;
    public final static IRI radian;
    public final static IRI unity;

    static {
        ValueFactory factory = SimpleValueFactory.getInstance();

        degree = factory.createIRI(UOM.NAMESPACE, "degree");
        GridSpacing = factory.createIRI(UOM.NAMESPACE, "GridSpacing");
        metre = factory.createIRI(UOM.NAMESPACE, "metre");
        radian = factory.createIRI(UOM.NAMESPACE, "radian");
        unity = factory.createIRI(UOM.NAMESPACE, "unity");
    }

}
