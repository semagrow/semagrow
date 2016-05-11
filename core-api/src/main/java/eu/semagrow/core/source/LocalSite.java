package eu.semagrow.core.source;

import org.openrdf.model.Resource;
import org.openrdf.model.impl.ValueFactoryImpl;


/**
 * Created by angel on 5/4/2016.
 */
public class LocalSite implements Site {

    static Site s = new LocalSite();

    @Override
    public String getType() {
        return "SEMAGROW" ;
    }

    public Resource getID() { return ValueFactoryImpl.getInstance().createBNode(); }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public boolean isRemote() {
        return false;
    }

    @Override
    public SourceCapabilities getCapabilities() {
        return new SourceCapabilitiesBase();
    }

    public static Site getInstance() {
        return s;
    }
}
