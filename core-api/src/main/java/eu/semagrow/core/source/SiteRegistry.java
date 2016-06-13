package eu.semagrow.core.source;

import org.eclipse.rdf4j.common.lang.service.ServiceRegistry;

/**
 * Created by angel on 6/4/2016.
 */
public class SiteRegistry extends ServiceRegistry<String,SiteFactory> {

    private static SiteRegistry registry;

    public static synchronized SiteRegistry getInstance() {
        if (registry == null)
            registry = new SiteRegistry();

        return registry;
    }

    protected SiteRegistry() {
        super(SiteFactory.class);
    }

    @Override
    protected String getKey(SiteFactory siteFactory) { return siteFactory.getType(); }

}
