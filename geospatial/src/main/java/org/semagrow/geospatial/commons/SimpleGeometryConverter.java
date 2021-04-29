package org.semagrow.geospatial.commons;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;
import org.locationtech.jts.io.gml2.GMLReader;
import org.locationtech.jts.io.gml2.GMLWriter;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

public final class SimpleGeometryConverter {

    private static final WKTReader wktReader = new WKTReader();
    private static final GMLReader gmlReader = new GMLReader();
    private static final WKTWriter wktWriter = new WKTWriter();
    private static final GMLWriter gmlWriter = new GMLWriter();
    private static final GeometryFactory factory = new GeometryFactory();

    private static final SimpleGeometryConverter instance = new SimpleGeometryConverter();

    public static SimpleGeometryConverter getInstance() {
        return instance;
    }

    public String GMLtoWKT(String in) throws IllegalGeometryException {
        Geometry geometry = null;
        try {
            geometry = gmlReader.read(in, factory);
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
            throw new IllegalGeometryException(e);
        } catch (ParserConfigurationException e) {
            e.printStackTrace();
            throw new IllegalGeometryException(e);
        }
        return wktWriter.write(geometry);
    }

    public String WKTtoGML(String in) throws IllegalGeometryException {
        Geometry geometry = null;
        try {
            geometry = wktReader.read(in);
            gmlWriter.write(geometry);
        } catch (ParseException e) {
            e.printStackTrace();
            throw new IllegalGeometryException(e);
        }
        return wktWriter.write(geometry);
    }
}
