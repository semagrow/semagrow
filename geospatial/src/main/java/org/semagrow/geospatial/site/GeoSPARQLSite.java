package org.semagrow.geospatial.site;

import org.semagrow.connector.sparql.SPARQLSite;

import java.net.URL;

public class GeoSPARQLSite extends SPARQLSite {
    public GeoSPARQLSite(URL uri) {
        super(uri);
    }

    @Override
    public String getType() { return "GeoSPARQL"; }
}
