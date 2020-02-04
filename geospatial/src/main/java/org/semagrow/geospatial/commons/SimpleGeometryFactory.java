package org.semagrow.geospatial.commons;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

public class SimpleGeometryFactory {
	
    private static final WKTReader wktReader = new WKTReader();
    
    private static final SimpleGeometryFactory instance = new SimpleGeometryFactory();

    public static SimpleGeometryFactory getInstance() {
        return instance;
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
