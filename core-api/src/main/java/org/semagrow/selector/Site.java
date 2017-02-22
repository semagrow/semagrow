package org.semagrow.selector;

import org.eclipse.rdf4j.model.Resource;

/**
 * Created by angel on 5/4/2016.
 */
public interface Site {

    String getType();

    Resource getID();

    boolean isRemote();

    boolean equals(Object o);

    SiteCapabilities getCapabilities();

}
