package org.semagrow.selector;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;

/**
 * Created by angel on 5/4/2016.
 */
public interface SiteConfig {

    String getType();

    void validate();

    String getSiteId();

    void setSiteId(String id);

    Resource export(Model graph);

    void parse(Model graph, Resource resource);

}
