package eu.semagrow.stack.modules.sails.semagrow.optimizer;

import eu.semagrow.stack.modules.utils.resourceselector.ResourceSelector;
import eu.semagrow.stack.modules.api.source.SourceSelector;
import eu.semagrow.stack.modules.api.statistics.Statistics;
import eu.semagrow.stack.modules.querydecomp.selector.SourceSelectorAdapter;
import eu.semagrow.stack.modules.querydecomp.selector.VOIDStatistics;
import eu.semagrow.stack.modules.sails.semagrow.TrivialResourceSelector;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CardinalityEstimatorImpl;
import eu.semagrow.stack.modules.api.estimator.CostEstimator;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CostEstimatorImpl;
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

}
