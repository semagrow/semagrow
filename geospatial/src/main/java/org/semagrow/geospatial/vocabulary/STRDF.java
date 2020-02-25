package org.semagrow.geospatial.vocabulary;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Namespace;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleNamespace;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public final class STRDF {

    public static final String NAMESPACE = "http://strdf.di.uoa.gr/ontology#";

    public static final String PREFIX = "strdf";

    public static final Namespace NS = new SimpleNamespace(PREFIX, NAMESPACE);

    public final static IRI dimension;
    public final static IRI geometryType;
    public final static IRI srid;
    public final static IRI asText;
    public final static IRI asGML;
    public final static IRI isEmpty;
    public final static IRI isSimple;

    public final static IRI equals;
    public final static IRI disjoint;
    public final static IRI intersects;
    public final static IRI touches;
    public final static IRI crosses;
    public final static IRI within;
    public final static IRI contains;
    public final static IRI overlaps;
    public final static IRI relate;

    public final static IRI mbbIntersects;
    public final static IRI mbbEquals;
    public final static IRI mbbWithin;
    public final static IRI mbbContains;

    public final static IRI left;
    public final static IRI right;
    public final static IRI above;
    public final static IRI below;

    public final static IRI buffer;
    public final static IRI boundary;
    public final static IRI envelope;
    public final static IRI convexHull;
    public final static IRI intersection;
    public final static IRI union;
    public final static IRI difference;
    public final static IRI symDifference;
    public final static IRI extent;

    public final static IRI distance;
    public final static IRI area;

    static {
        ValueFactory factory = SimpleValueFactory.getInstance();

        dimension = factory.createIRI(STRDF.NAMESPACE, "dimension");
        geometryType = factory.createIRI(STRDF.NAMESPACE, "geometryType");
        srid = factory.createIRI(STRDF.NAMESPACE, "srid");
        asText = factory.createIRI(STRDF.NAMESPACE, "asText");
        asGML = factory.createIRI(STRDF.NAMESPACE, "asGML");
        isEmpty = factory.createIRI(STRDF.NAMESPACE, "isEmpty");
        isSimple = factory.createIRI(STRDF.NAMESPACE, "isSimple");

        equals = factory.createIRI(STRDF.NAMESPACE, "equals");
        disjoint = factory.createIRI(STRDF.NAMESPACE, "disjoint");
        intersects = factory.createIRI(STRDF.NAMESPACE, "intersects");
        touches = factory.createIRI(STRDF.NAMESPACE, "touches");
        crosses = factory.createIRI(STRDF.NAMESPACE, "crosses");
        within = factory.createIRI(STRDF.NAMESPACE, "within");
        contains = factory.createIRI(STRDF.NAMESPACE, "contains");
        overlaps = factory.createIRI(STRDF.NAMESPACE, "overlaps");
        relate = factory.createIRI(STRDF.NAMESPACE, "relate");

        mbbIntersects = factory.createIRI(STRDF.NAMESPACE, "mbbIntersects");
        mbbEquals = factory.createIRI(STRDF.NAMESPACE, "mbbEquals");
        mbbWithin = factory.createIRI(STRDF.NAMESPACE, "mbbWithin");
        mbbContains = factory.createIRI(STRDF.NAMESPACE, "mbbContains");

        left = factory.createIRI(STRDF.NAMESPACE, "left");
        right = factory.createIRI(STRDF.NAMESPACE, "right");
        above = factory.createIRI(STRDF.NAMESPACE, "above");
        below = factory.createIRI(STRDF.NAMESPACE, "below");

        buffer = factory.createIRI(STRDF.NAMESPACE, "buffer");
        boundary = factory.createIRI(STRDF.NAMESPACE, "boundary");
        envelope = factory.createIRI(STRDF.NAMESPACE, "envelope");
        convexHull = factory.createIRI(STRDF.NAMESPACE, "convexHull");
        intersection = factory.createIRI(STRDF.NAMESPACE, "intersection");
        union = factory.createIRI(STRDF.NAMESPACE, "union");
        difference = factory.createIRI(STRDF.NAMESPACE, "difference");
        symDifference = factory.createIRI(STRDF.NAMESPACE, "symDifference");
        extent = factory.createIRI(STRDF.NAMESPACE, "extent");

        distance = factory.createIRI(STRDF.NAMESPACE, "distance");
        area = factory.createIRI(STRDF.NAMESPACE, "area");
    }
}
