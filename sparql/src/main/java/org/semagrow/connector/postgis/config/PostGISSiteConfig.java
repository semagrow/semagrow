package org.semagrow.connector.postgis.config;

import org.semagrow.selector.SiteConfig;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

public class PostGISSiteConfig implements SiteConfig {

    public static String TYPE = "POSTGIS";
    private String siteId;

    public String getType() {
        return null;
    }

    public void validate() {

    }

    public String getSiteId() {
        return siteId;
    }

    public void setSiteId(String id) {
        siteId = id;
    }

    public Resource export(Model graph) {
        return null;
    }

    public void parse(Model graph, Resource resource) {

    }
}
