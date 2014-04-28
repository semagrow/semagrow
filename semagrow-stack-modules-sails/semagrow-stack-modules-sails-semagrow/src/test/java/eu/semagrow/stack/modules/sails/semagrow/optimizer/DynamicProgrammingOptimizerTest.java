package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CardinalityEstimatorImpl;
import eu.semagrow.stack.modules.querydecomp.estimator.CostEstimator;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CostEstimatorImpl;
import eu.semagrow.stack.modules.querydecomp.selector.mock.TrivialResourceSelector;
import junit.framework.TestCase;
import org.junit.Before;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;

/**
 * Created by angel on 4/27/14.
 */
public class DynamicProgrammingOptimizerTest extends TestCase {


    private ResourceSelector selector;
    private CostEstimator costEstimator;

    @Before
    public void setUp() {
        selector = new TrivialResourceSelector();
        costEstimator = new CostEstimatorImpl(new CardinalityEstimatorImpl(selector), selector);
    }

    public void testsimpleTest() throws Exception {

        String q = "SELECT ?y ?o WHERE { ?y <http://localhost/my1> ?s1." +
                "?s1 <http://localhost/my2> ?o. FILTER (regex(?y, 'a') && regex(?o, 'b')). }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery query = parser.parseQuery(q, null);

        ResourceSelector selector = new TrivialResourceSelector();
        DynamicProgrammingOptimizer optimizer =
                new DynamicProgrammingOptimizer(costEstimator, selector);
        TupleExpr expr = query.getTupleExpr();
        System.out.println(expr);
        new ConjunctiveConstraintSplitter().optimize(expr, query.getDataset(), null);
        optimizer.optimize(expr, query.getDataset(), null);

        System.out.println(expr);

    }


    public void testOptimizebpg() throws Exception {

    }
}
