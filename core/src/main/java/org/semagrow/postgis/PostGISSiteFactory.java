package org.semagrow.postgis;

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
    	if (config instanceof PostGISSiteConfig) {
    		PostGISSiteConfig postgisSiteConfig = (PostGISSiteConfig)config;
            return new PostGISSite(postgisSiteConfig.getEndpoint());
    	}
        else
        	throw new IllegalArgumentException("config is not of type PostGISSiteConfig");
    }

}
