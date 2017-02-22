package org.semagrow.local;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.semagrow.model.SemagrowValueFactory;
import org.semagrow.selector.Site;
import org.semagrow.selector.SiteCapabilities;


/**
 * Created by angel on 5/4/2016.
 */
public class LocalSite implements Site {

    private final static Site sharedInstance = new LocalSite();

    private final static String TYPE = "SEMAGROW";

    private Resource id = SemagrowValueFactory.getInstance().createBNode();

    protected LocalSite() { }

    public String getType() {
        return TYPE ;
    }

    public Resource getID() { return id; }

    public boolean isRemote() {
        return false;
    }

    public SiteCapabilities getCapabilities() {
        return new LocalSiteCapabilities();
    }

    public static Site getInstance() {
        return sharedInstance;
    }

    @Override
    public String toString() { return "local-semagrow"; }

}
