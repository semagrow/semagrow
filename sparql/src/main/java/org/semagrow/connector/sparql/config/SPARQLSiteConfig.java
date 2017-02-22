package org.semagrow.connector.sparql.config;

import org.semagrow.selector.SiteConfig;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

/**
 * Created by angel on 6/4/2016.
 */
public class SPARQLSiteConfig implements SiteConfig {

    public static String TYPE = "SPARQL";
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
