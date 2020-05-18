package org.semagrow.postgis;

import org.semagrow.selector.SiteConfig;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

public class PostGISSiteConfig implements SiteConfig {

    public static String TYPE = "POSTGIS";
    private String siteId;
    private IRI endpoint;

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void validate() { }

    @Override
    public String getSiteId() {
        return siteId;
    }

    @Override
    public void setSiteId(String id) {
        siteId = id;
        setEndpoint(SimpleValueFactory.getInstance().createIRI(id));
    }

    public Resource export(Model graph) {
        return null;
    }

    public void parse(Model graph, Resource resource) { }
    
    public IRI getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(IRI endpoint) {
        this.endpoint = endpoint;
    }
}
