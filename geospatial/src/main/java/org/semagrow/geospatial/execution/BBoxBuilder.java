package org.semagrow.geospatial.execution;

import com.esri.core.geometry.*;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.vocabulary.GEO;
import org.semagrow.geospatial.helpers.GeoLocation;
import org.semagrow.geospatial.vocabulary.UOM;

import static com.esri.core.geometry.WktExportFlags.wktExportDefaults;
import static com.esri.core.geometry.WktImportFlags.wktImportDefaults;

public final class BBoxBuilder {

    public static Value getBufferedBBox(String wkt, double distance, IRI uom) {

        ValueFactory vf = SimpleValueFactory.getInstance();
        Geometry geom = OperatorImportFromWkt.local().execute(wktImportDefaults, Geometry.Type.Unknown, wkt, null);
        Geometry bbox = new Polygon();

        if (uom.equals(UOM.degree)) {
            bbox = getBufferedBBoxDegrees(geom, distance);
        }
        if (uom.equals(UOM.metre)) {
            bbox = getBufferedBBoxMeters(geom, distance);
        }

        String str = OperatorExportToWkt.local().execute(wktExportDefaults, bbox, null);
        Value value = vf.createLiteral(str, GEO.WKT_LITERAL);
        return value;
    }

    private static Geometry getBufferedBBoxDegrees(Geometry g, double distance) {
        /* http://esri.github.io/geometry-api-java/doc/Buffer.html */
        SpatialReference spatialRef = SpatialReference.create(4326);
        Geometry b = OperatorBuffer.local().execute(g, spatialRef, distance, null);
        Envelope e = new Envelope();
        b.queryEnvelope(e);
        return e;
    }

    private static Geometry getBufferedBBoxMeters(Geometry g, double distance) {
        Envelope e = new Envelope();
        g.queryEnvelope(e);

        Point point;
        GeoLocation location;
        GeoLocation[] bounding;
        MultiPoint mPoint = new MultiPoint();

        double radius = 6371010 / 1.5;

        /* lower left point */
        point = e.getLowerLeft();
        location = GeoLocation.fromDegrees(point.getX(), point.getY());
        bounding = location.boundingCoordinates(distance, radius);
        mPoint.add(bounding[0].getLatitudeInDegrees(), bounding[0].getLongitudeInDegrees());
        mPoint.add(bounding[1].getLatitudeInDegrees(), bounding[1].getLongitudeInDegrees());

        /* upper right point */
        point = e.getUpperRight();
        location = GeoLocation.fromDegrees(point.getX(), point.getY());
        bounding = location.boundingCoordinates(distance, radius);
        mPoint.add(bounding[0].getLatitudeInDegrees(), bounding[0].getLongitudeInDegrees());
        mPoint.add(bounding[1].getLatitudeInDegrees(), bounding[1].getLongitudeInDegrees());

        System.out.println(mPoint);

        Envelope b = new Envelope();
        mPoint.queryEnvelope(b);

        return b;
    }



}
