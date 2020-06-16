package org.semagrow.connector.postgis.config;

import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.semagrow.connector.postgis.PostGISSite;
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
			return new PostGISSite(SimpleValueFactory.getInstance().createIRI(config.getSiteId()));
    }

}
