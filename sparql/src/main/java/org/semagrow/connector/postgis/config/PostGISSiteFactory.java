package org.semagrow.connector.postgis.config;

import org.semagrow.connector.postgis.PostGISSite;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteConfig;
import org.semagrow.selector.SiteFactory;

import java.net.MalformedURLException;
import java.net.URL;

public class PostGISSiteFactory implements SiteFactory {

    @Override
    public String getType() { 
    	return PostGISSiteConfig.TYPE; 
    }

    @Override
    public SiteConfig getConfig() { 
    	return new PostGISSiteConfig(); 
    }

    @Override
    public Site getSite(SiteConfig config) {
        try {
            URL u = new URL(config.getSiteId());
            return new PostGISSite(u);
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Configuration is not a valid URL");
        }
    }

}
