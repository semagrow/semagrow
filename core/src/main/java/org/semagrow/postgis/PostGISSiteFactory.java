package org.semagrow.postgis;

import java.net.MalformedURLException;
import java.net.URL;

import org.semagrow.postgis.PostGISSite;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteConfig;
import org.semagrow.selector.SiteFactory;

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
//    	if (config instanceof PostGISSiteConfig) {
//    		PostGISSiteConfig postgisSiteConfig = (PostGISSiteConfig)config;
    		URL u;
			try {
				u = new URL(config.getSiteId());
				return new PostGISSite(u);
			} catch (MalformedURLException e) {
	            throw new IllegalArgumentException("Configuration is not a valid URL");
			}
//            return new PostGISSite(postgisSiteConfig.getEndpoint());
//    	}
//        else
//        	throw new IllegalArgumentException("config is not of type PostGISSiteConfig");
    }

}
