package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.querydecomp.selector.mock.TrivialResourceSelector;
import junit.framework.TestCase;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.query.parser.sparql.SPARQLParserFactory;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.memory.MemoryStore;

/**
 * Created by angel on 4/27/14.
 */
public class DynamicProgrammingOptimizerTest extends TestCase {


    public void testsimpleTest() throws Exception {

        String q = "SELECT ?s ?o WHERE { ?s <http://localhost/my1> ?s1." +
                "?s1 <http://localhost/my2> ?o }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery query = parser.parseQuery(q, null);

        DynamicProgrammingOptimizer optimizer =
                new DynamicProgrammingOptimizer(null, new TrivialResourceSelector());
        TupleExpr expr = query.getTupleExpr();
        System.out.println(expr);
        optimizer.optimize(query.getTupleExpr(), query.getDataset(), null);

        System.out.println(expr);

    }


    public void testOptimizebpg() throws Exception {

    }
}
