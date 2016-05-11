package eu.semagrow.cassandra.config;

import eu.semagrow.cassandra.CassandraSite;
import eu.semagrow.core.source.Site;
import eu.semagrow.core.source.SiteConfig;
import eu.semagrow.core.source.SiteFactory;
import org.openrdf.model.URI;

/**
 * Created by angel on 5/4/2016.
 */
public class CassandraSiteFactory implements SiteFactory {

    @Override
    public String getType() {
        return CassandraSiteConfig.TYPE;
    }

    @Override
    public SiteConfig getConfig() {
        return new CassandraSiteConfig();
    }

    @Override
    public Site getSite(SiteConfig config) {
        if (config instanceof CassandraSiteConfig) {
            CassandraSiteConfig cassandraSiteConfig = (CassandraSiteConfig)config;
            return new CassandraSite(cassandraSiteConfig.getEndpoint());
        }
        else
            throw new IllegalArgumentException("config is not of type CassandraSiteConfig");

    }

    @Override
    public Site getSite(URI endpoint) {
        return new CassandraSite(endpoint);
    }
}
