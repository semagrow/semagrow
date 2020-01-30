package org.semagrow.geospatial.commons;

import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.io.WKTReader;
import org.locationtech.spatial4j.shape.Shape;

import java.text.ParseException;
import java.util.IllegalFormatException;

public final class SimpleShapeFactory {

    private static final JtsSpatialContextFactory jtsSpatialContextFactory = new JtsSpatialContextFactory();
    private static final JtsSpatialContext jtsSpatialContext = jtsSpatialContextFactory.newSpatialContext();
    private static final WKTReader reader = new WKTReader(jtsSpatialContext, jtsSpatialContextFactory);

    private static final SimpleShapeFactory instance = new SimpleShapeFactory();

    public static SimpleShapeFactory getInstance() {
        return instance;
    }

    public static Shape createShape(String wktString) throws IllegalGeometryException {
        try {
            return reader.parse(wktString);
        } catch (ParseException e) {
            throw new IllegalGeometryException(e);
        }
    }
}
