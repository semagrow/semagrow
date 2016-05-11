package eu.semagrow.core.impl.sparql;

import eu.semagrow.core.source.SiteConfig;
import org.openrdf.model.Graph;
import org.openrdf.model.Resource;

/**
 * Created by angel on 6/4/2016.
 */
public class SPARQLSiteConfig implements SiteConfig {

    public static String TYPE = "SPARQL";

    @Override
    public String getType() {
        return null;
    }

    @Override
    public void validate() {

    }

    @Override
    public Resource export(Graph graph) {
        return null;
    }

    @Override
    public void parse(Graph graph, Resource resource) {

    }

}
