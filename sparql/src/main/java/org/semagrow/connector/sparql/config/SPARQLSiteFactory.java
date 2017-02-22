package org.semagrow.connector.sparql.config;

import org.semagrow.connector.sparql.SPARQLSite;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteConfig;
import org.semagrow.selector.SiteFactory;

import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by angel on 6/4/2016.
 */
public class SPARQLSiteFactory implements SiteFactory {

    @Override
    public String getType() { return SPARQLSiteConfig.TYPE; }

    @Override
    public SiteConfig getConfig() { return new SPARQLSiteConfig(); }

    @Override
    public Site getSite(SiteConfig config) {
        try {
            URL u = new URL(config.getSiteId());
            return new SPARQLSite(u);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Configuration is not a valid URL");
        }
    }

}
