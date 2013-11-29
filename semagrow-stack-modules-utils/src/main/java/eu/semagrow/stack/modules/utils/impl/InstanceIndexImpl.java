/**
 * 
 */
package eu.semagrow.stack.modules.utils.impl;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import eu.semagrow.stack.modules.utils.InstanceIndex;
import eu.semagrow.stack.modules.utils.SelectedResource;

import java.util.ArrayList;
import java.util.List;

/* (non-Javadoc)
 * @see eu.semagrow.stack.modules.utils.InstanceIndex
 */
public class InstanceIndexImpl implements InstanceIndex {
	
	/**
	 * 
	 */
	public InstanceIndexImpl() {
		super();
	}

	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.InstanceIndexInterface#getEndpoints(org.openrdf.model.URI)
	 */
	public List<SelectedResource> getEndpoints(URI uri) {//TODO:remove dummy	
		List<SelectedResource> list = new ArrayList<SelectedResource>();
		ValueFactory valueFactory = new ValueFactoryImpl();
		SelectedResource selectedResource1 = new SelectedResourceImpl(valueFactory.createURI("http://a"), 100, 1);
		SelectedResource selectedResource2 = new SelectedResourceImpl(valueFactory.createURI("http://b"), 10, 2);
		SelectedResource selectedResource3 = new SelectedResourceImpl(valueFactory.createURI("http://c"), 100, 1);
		list.add(selectedResource1);
		list.add(selectedResource2);
		list.add(selectedResource3);
		return list;
	}

}
