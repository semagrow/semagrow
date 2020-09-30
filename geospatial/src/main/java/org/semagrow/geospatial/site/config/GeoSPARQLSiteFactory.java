package org.semagrow.geospatial.site.config;

import org.semagrow.geospatial.site.GeoSPARQLSite;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteConfig;
import org.semagrow.selector.SiteFactory;

import java.net.MalformedURLException;
import java.net.URL;

public class GeoSPARQLSiteFactory implements SiteFactory {
    @Override
    public String getType() { return GeoSPARQLSiteConfig.TYPE; }

    @Override
    public SiteConfig getConfig() { return new GeoSPARQLSiteConfig(); }

    @Override
    public Site getSite(SiteConfig config) {
        try {
            URL u = new URL(config.getSiteId());
            return new GeoSPARQLSite(u);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Configuration is not a valid URL");
        }
    }
}
