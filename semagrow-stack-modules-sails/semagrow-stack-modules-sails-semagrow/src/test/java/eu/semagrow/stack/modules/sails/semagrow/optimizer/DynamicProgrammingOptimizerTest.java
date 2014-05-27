package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.querydecomp.SourceSelector;
import eu.semagrow.stack.modules.querydecomp.Statistics;
import eu.semagrow.stack.modules.querydecomp.selector.SourceSelectorAdapter;
import eu.semagrow.stack.modules.querydecomp.selector.VOIDStatistics;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CardinalityEstimatorImpl;
import eu.semagrow.stack.modules.querydecomp.estimator.CostEstimator;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CostEstimatorImpl;
import eu.semagrow.stack.modules.querydecomp.selector.mock.TrivialResourceSelector;
import info.aduna.iteration.CloseableIteration;
import junit.framework.TestCase;
import org.junit.Before;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.TupleExpr;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.parser.ParsedQuery;
import org.openrdf.query.parser.sparql.SPARQLParser;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.sparql.SPARQLRepository;

import java.util.logging.Logger;

/**
 * Created by angel on 4/27/14.
 */
public class DynamicProgrammingOptimizerTest extends TestCase {


    private ResourceSelector selector;
    private CostEstimator costEstimator;

    @Before
    public void setUp() {
        selector = new TrivialResourceSelector();
        Statistics statistics = new VOIDStatistics(null);
        costEstimator = new CostEstimatorImpl(new CardinalityEstimatorImpl(statistics));
    }

    public void testsimpleTest() throws Exception {

        String q = "SELECT ?y ?o WHERE { " +
                "?y <http://localhost/my1> ?s1." +
                "?s1 <http://localhost/my2> ?o. " +
                "FILTER (regex(?y, 'a') && regex(?o, 'b')). }";

        SPARQLParser parser = new SPARQLParser();
        ParsedQuery query = parser.parseQuery(q, null);

        SourceSelector selector = new SourceSelectorAdapter(new TrivialResourceSelector());
        DynamicProgrammingOptimizer optimizer =
                new DynamicProgrammingOptimizer(costEstimator, selector);
        TupleExpr expr = query.getTupleExpr();
        System.out.println(expr);
        new ConjunctiveConstraintSplitter().optimize(expr, query.getDataset(), null);
        optimizer.optimize(expr, query.getDataset(), null);

        System.out.println(expr);

    }

    public void testAsync() throws Exception {

        Logger log = Logger.getAnonymousLogger();

        SPARQLRepository repository1 = new SPARQLRepository("http://localhost:8080/openrdf-sesame/repositories/fedx");
        SPARQLRepository repository2 = new SPARQLRepository("http://localhost:8080/openrdf-sesame/repositories/fedx2");
        repository1.initialize();
        repository2.initialize();

        RepositoryConnection con = repository1.getConnection();
        RepositoryConnection con2 = repository2.getConnection();

        log.info("Repository initialized");

        String q = "SELECT * WHERE {?s ?p ?o}";
        TupleQuery q1 = con.prepareTupleQuery(QueryLanguage.SPARQL, q);
        TupleQuery q2 = con2.prepareTupleQuery(QueryLanguage.SPARQL, q);

        log.info("query preparation done");

        log.info("start of eval q2");
        TupleQueryResult result2 = q2.evaluate();
        log.info("end of eval q2");
        log.info("start of eval q1");
        TupleQueryResult result = q1.evaluate();
        log.info("end of eval q1");

        while (result2.hasNext()) {
            BindingSet b = result2.next();
            System.out.print(b);
        }
    }

    public void testOptimizebpg() throws Exception {

    }
}
