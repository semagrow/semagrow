/**
 * 
 */
package eu.semagrow.stack.modules.utils.resourceselector.impl;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import eu.semagrow.stack.modules.utils.resourceselector.SchemaIndex;
import eu.semagrow.stack.modules.utils.resourceselector.SelectedResource;

/* (non-Javadoc)
 * @see eu.semagrow.stack.modules.utils.resourceselector.SchemaIndex
 */
public class SchemaIndexImpl implements SchemaIndex {
	
	/**
	 *
	 */
	public SchemaIndexImpl() {
		super();
	}

	/* (non-Javadoc)
	 * @see eu.semagrow.stack.modules.utils.resourceselector.SchemaIndex#getEndpoints(org.openrdf.model.URI)
	 */
	public List<SelectedResource> getEndpoints(URI uri) {//TODO:remove dummy
		List<SelectedResource> list = new ArrayList<SelectedResource>();
		ValueFactory valueFactory = new ValueFactoryImpl();
		SelectedResource selectedResource1 = new SelectedResourceImpl(valueFactory.createURI("http://a"), 100, 1);
		SelectedResource selectedResource2 = new SelectedResourceImpl(valueFactory.createURI("http://b"), 10, 2);
		SelectedResource selectedResource3 = new SelectedResourceImpl(valueFactory.createURI("http://c"), 100, 1);
		SelectedResource selectedResource4 = new SelectedResourceImpl(valueFactory.createURI("http://d"), 100, 1);
		SelectedResource selectedResource5 = new SelectedResourceImpl(valueFactory.createURI("http://e"), 100, 1);
		list.add(selectedResource1);
		list.add(selectedResource2);
		list.add(selectedResource3);
		list.add(selectedResource4);
		list.add(selectedResource5);
		return list;
	}

}
