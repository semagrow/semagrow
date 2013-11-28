/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Giannis Mouchakis
 *
 */
public class InstanceIndex {
	
	/**
	 * 
	 */
	public InstanceIndex() {
		super();
	}

	public List<SelectedResource> getEndpoints(URI uri) {//TODO:remove dummy	
		List<SelectedResource> list = new ArrayList<SelectedResource>();
		ValueFactory valueFactory = new ValueFactoryImpl();
		SelectedResource selectedResource1 = new SelectedResource(valueFactory.createURI("http://a"), 100, 1);
		SelectedResource selectedResource2 = new SelectedResource(valueFactory.createURI("http://b"), 10, 2);
		SelectedResource selectedResource3 = new SelectedResource(valueFactory.createURI("http://c"), 100, 1);
		list.add(selectedResource1);
		list.add(selectedResource2);
		list.add(selectedResource3);
		return list;
	}

}
