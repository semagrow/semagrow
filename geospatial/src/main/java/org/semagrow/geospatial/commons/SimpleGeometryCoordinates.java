package org.semagrow.geospatial.commons;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;

public class SimpleGeometryCoordinates {
	/* What happens if Geometry is empty? */
	
    public static double maxX(Geometry mbb) {
        Coordinate[] coords = mbb.getCoordinates();
        double maxX = 0;
        if (coords.length == 1) {
        	maxX = coords[0].x;
        }
        else if (coords.length == 2) {
        	maxX = (coords[0].x > coords[1].x) ? coords[0].x : coords[1].x;
        }
        else {	// Polygon whose vertices are (minx miny, maxx miny, maxx maxy, minx maxy, minx miny)
        	maxX = coords[1].x;
        }
        return maxX;
    }
    
	public static double minX(Geometry mbb) {
        Coordinate[] coords = mbb.getCoordinates();
        double minX = 0;
        if (coords.length == 1) {
        	minX = coords[0].x;
        }
        else if (coords.length == 2) {
        	minX = (coords[0].x < coords[1].x) ? coords[0].x : coords[1].x;
        }
        else {
        	minX = coords[0].x;
        }
        return minX;
    }

	public static double maxY(Geometry mbb) {
	    Coordinate[] coords = mbb.getCoordinates();
	    double maxY = 0;
	    if (coords.length == 1) {
	    	maxY = coords[0].y;
	    }
	    else if (coords.length == 2) {
	    	maxY = (coords[0].y > coords[1].y) ? coords[0].y : coords[1].y;
	    }
	    else {
	    	maxY = coords[2].y;
	    }
	    return maxY;
	}
	
	public static double minY(Geometry mbb) {
	    Coordinate[] coords = mbb.getCoordinates();
	    double minY = 0;
	    if (coords.length == 1) {
	    	minY = coords[0].y;
	    }
	    else if (coords.length == 2) {
	    	minY = (coords[0].y < coords[1].y) ? coords[0].y : coords[1].y;
	    }
	    else {
	    	minY = coords[0].y;
	    }
	    return minY;
	}
}