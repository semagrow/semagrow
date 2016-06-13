package eu.semagrow.core.impl.sparql;

import eu.semagrow.core.source.SiteConfig;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

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
    public Resource export(Model graph) {
        return null;
    }

    @Override
    public void parse(Model graph, Resource resource) {

    }

}
