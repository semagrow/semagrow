package org.semagrow.selector;

import org.eclipse.rdf4j.model.IRI;

/**
 * Created by angel on 5/4/2016.
 */
public interface SiteFactory {

    String getType();

    SiteConfig getConfig();

    Site getSite(SiteConfig config);
}
