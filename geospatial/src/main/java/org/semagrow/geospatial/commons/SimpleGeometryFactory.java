package org.semagrow.geospatial.commons;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.spatial4j.context.jts.JtsSpatialContext;
import org.locationtech.spatial4j.context.jts.JtsSpatialContextFactory;
import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.jts.JtsShapeFactory;

public class SimpleGeometryFactory {
	
	private static final JtsSpatialContextFactory jtsSpatialContextFactory = new JtsSpatialContextFactory();
    private static final JtsSpatialContext jtsSpatialContext = jtsSpatialContextFactory.newSpatialContext();
    private static final JtsShapeFactory jtsShapeFactory = new JtsShapeFactory(jtsSpatialContext, jtsSpatialContextFactory);
    private static final WKTReader wktReader = new WKTReader();
    
    private static final SimpleGeometryFactory instance = new SimpleGeometryFactory();

    public static SimpleGeometryFactory getInstance() {
        return instance;
    }
    
    public static Geometry createGeometry(Shape shape) {
    	return jtsShapeFactory.getGeometryFrom(shape);
    }
    
    public static Geometry createGeometry(String wkt) throws IllegalGeometryException {
        Geometry geometry = null;
        try {
            geometry = wktReader.read(wkt);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new IllegalGeometryException(e);
        }
        return geometry;
    }	
}
