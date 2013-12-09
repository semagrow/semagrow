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
public class FirstTest {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws SQLException  
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException {
		
		ValueFactory vf = new ValueFactoryImpl();
		Resource a = vf.createURI("http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/101/onto.rdf#abstract");
		Var subject = new Var("a", a);
		Literal b = vf.createLiteral("foobara");
		Var predicate = new Var("b", b);
		Literal c = vf.createLiteral("foobarb");
		Var object = new Var("c", c);
		StatementPattern statementPattern = new StatementPattern(subject, predicate, object);
		System.out.println(statementPattern);
		
		ResourceSelector resourceSelector = new ResourceSelectorImpl();
		System.out.println(resourceSelector.getSelectedResources(statementPattern, 0));

	}

}
