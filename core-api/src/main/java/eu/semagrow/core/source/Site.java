package eu.semagrow.core.source;

import org.openrdf.model.Resource;

/**
 * Created by angel on 5/4/2016.
 */
public interface Site {

    String getType();

    Resource getID();

    boolean isLocal();

    boolean isRemote();

    boolean equals(Object o);

    SourceCapabilities getCapabilities();

}
