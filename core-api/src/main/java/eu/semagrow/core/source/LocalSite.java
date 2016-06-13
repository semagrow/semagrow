package eu.semagrow.core.source;

import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;


/**
 * Created by angel on 5/4/2016.
 */
public class LocalSite implements Site {

    static Site s = new LocalSite();

    @Override
    public String getType() {
        return "SEMAGROW" ;
    }

    public Resource getID() { return SimpleValueFactory.getInstance().createBNode(); }

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
