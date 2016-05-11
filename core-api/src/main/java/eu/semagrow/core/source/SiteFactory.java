package eu.semagrow.core.source;

import org.openrdf.model.URI;

/**
 * Created by angel on 5/4/2016.
 */
public interface SiteFactory {

    String getType();

    SiteConfig getConfig();

    Site getSite(SiteConfig config);

    Site getSite(URI endpoint);
}
