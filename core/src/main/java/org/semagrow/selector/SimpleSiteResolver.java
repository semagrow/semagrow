package org.semagrow.selector;

import java.util.Optional;

/**
 * Created by angel on 19/6/2016.
 */
public class SimpleSiteResolver implements SiteResolver {

    private final SiteRegistry siteRegistry;

    public SimpleSiteResolver() {
        siteRegistry = SiteRegistry.getInstance();
    }


    public Site getSite(String id) {

        Optional<SiteFactory> maybeSiteFactory;

        if (id.contains("cassandra")) {
            maybeSiteFactory = siteRegistry.get("CASSANDRA");
        }
        else if (id.contains("postgresql")) {
        	maybeSiteFactory = siteRegistry.get("POSTGIS");
        }
        else {
            maybeSiteFactory = siteRegistry.get("SPARQL");
        }

        if (maybeSiteFactory.isPresent()) {
            SiteFactory siteFactory = maybeSiteFactory.get();
            SiteConfig siteConfig = siteFactory.getConfig();
            siteConfig.setSiteId(id);
            return siteFactory.getSite(siteConfig);
        }
        else
            return null;
    }

}
