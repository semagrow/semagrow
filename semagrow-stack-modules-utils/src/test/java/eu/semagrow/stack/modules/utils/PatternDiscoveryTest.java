/**
 * 
 */
package eu.semagrow.stack.modules.utils;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Giannis Mouchakis
 *
 */
public class PatternDiscoveryTest {
	
	ArrayList<EquivalentURI> expected;
	PatternDiscovery patternDiscovery;

	@Before
	public void setUp() throws URISyntaxException {
		expected = new ArrayList<EquivalentURI>();
		EquivalentURI equivalentURI = new EquivalentURI(new URI("http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/201/onto.rdf#tiadthaumjrqaltngfuvjllglf"), 1000, new URI("http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/201/onto.rdf"));
		expected.add(equivalentURI);
		patternDiscovery = new PatternDiscovery(new URI("http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/101/onto.rdf#abstract"));
	}
	
	/**
	 * Test method for {@link eu.semagrow.stack.modules.utils.PatternDiscovery#retrieveEquivalentPatterns()}.
	 * @throws URISyntaxException 
	 * @throws SQLException 
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 */
	@Test
	public void testRetrieveEquivalentPatterns() throws ClassNotFoundException, IOException, SQLException, URISyntaxException {
		String message = "Pattern Discovery for input "
				+ "uri http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/201/onto.rdf#tiadthaumjrqaltngfuvjllglf "
				+ "should return one result equivalent_URI=http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/201/onto.rdf#tiadthaumjrqaltngfuvjllglf,"
				+ " proximity=1000, schema=http://oaei.ontologymatching.org/2011/benchmarks/biblio/1/201/onto.rdf";
		ArrayList<EquivalentURI> actual = patternDiscovery.retrieveEquivalentPatterns();
		assertEquals(message, expected, actual);
	}

}
