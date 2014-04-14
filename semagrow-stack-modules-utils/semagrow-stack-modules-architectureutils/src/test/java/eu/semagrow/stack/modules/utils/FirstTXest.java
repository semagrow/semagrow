/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import java.io.IOException;
import java.sql.SQLException;

import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.StatementPattern;
import org.openrdf.query.algebra.Var;

import eu.semagrow.stack.modules.utils.resourceselector.ResourceSelector;
import eu.semagrow.stack.modules.utils.resourceselector.impl.ResourceSelectorImpl;

/**
 * @author Giannis Mouchakis
 *
 */
public class FirstTXest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException  
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		
		ValueFactory vf = new ValueFactoryImpl();
		Literal a = vf.createLiteral("foobara");
		Var subject = new Var("a", a);
		Resource b = vf.createURI("http://semagrow.eu/schemas/laflor#language");
		Var predicate = new Var("b", b);
		Resource c = vf.createURI("http://id.loc.gov/vocabulary/iso639-2/es#language");
		Var object = new Var("c", c);
		StatementPattern statementPattern = new StatementPattern(subject, predicate, object);
		//System.out.println(statementPattern);

		ResourceSelector resourceSelector = new ResourceSelectorImpl();
		resourceSelector.getSelectedResources(statementPattern, 1);
		//System.out.println(resourceSelector.getSelectedResources(statementPattern, 1));

	}

}
