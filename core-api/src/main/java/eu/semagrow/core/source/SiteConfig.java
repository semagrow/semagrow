package eu.semagrow.core.source;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

/**
 * Created by angel on 5/4/2016.
 */
public interface SiteConfig {

    String getType();

    void validate();

    Resource export(Model graph);

    void parse(Model graph, Resource resource);

}
