package org.semagrow.query;

import org.eclipse.rdf4j.query.BindingSet;

/**
 * Created by angel on 15/6/2016.
 */
public interface ProvenancedBindingSet extends BindingSet {

    Provenance getProvenance();

}
