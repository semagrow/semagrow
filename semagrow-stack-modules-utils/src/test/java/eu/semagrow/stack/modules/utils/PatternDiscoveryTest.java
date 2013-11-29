/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import eu.semagrow.stack.modules.utils.impl.EquivalentURIImpl;
import eu.semagrow.stack.modules.utils.impl.PatternDiscoveryImpl;

/**
 * @author Giannis Mouchakis
 *
 */
public class PatternDiscoveryTest {
	
	List<EquivalentURI> expected;
	PatternDiscovery patternDiscovery;

	@Before
	public void setUp() {
		expected = new ArrayList<EquivalentURI>();
		ValueFactory valueFactory = new ValueFactoryImpl();
		EquivalentURI equivalentURI = new EquivalentURIImpl(valueFactory.createURI("http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/201/onto.rdf#tiadthaumjrqaltngfuvjllglf"), 1000, valueFactory.createURI("http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/201/onto.rdf"));
		expected.add(equivalentURI);
		patternDiscovery = new PatternDiscoveryImpl(valueFactory.createURI("http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/101/onto.rdf#abstract"));
	}
	
	/**
	 * Test method for {@link eu.semagrow.stack.modules.utils.PatternDiscoveryImpl#retrieveEquivalentPatterns()}.
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testRetrieveEquivalentPatterns() throws ClassNotFoundException, IOException, SQLException {
		String message = "Pattern Discovery for input "
				+ "uri http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/201/onto.rdf#tiadthaumjrqaltngfuvjllglf "
				+ "should return one result equivalent_URI=http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/201/onto.rdf#tiadthaumjrqaltngfuvjllglf,"
				+ " proximity=1000, schema=http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/201/onto.rdf";
		List<EquivalentURI> actual = patternDiscovery.retrieveEquivalentPatterns();
		assertEquals(message, expected, actual);
	}

}
