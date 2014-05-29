package eu.semagrow.stack.modules.sails.semagrow;

import eu.semagrow.stack.modules.api.ResourceSelector;
import eu.semagrow.stack.modules.querydecomp.SourceSelector;
import eu.semagrow.stack.modules.querydecomp.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.querydecomp.estimator.CostEstimator;
import eu.semagrow.stack.modules.querydecomp.selector.SourceSelectorAdapter;
import eu.semagrow.stack.modules.querydecomp.selector.VOIDLoader;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CostEstimatorImpl;
import eu.semagrow.stack.modules.sails.semagrow.optimizer.DynamicProgrammingOptimizer;
import eu.semagrow.stack.modules.sails.semagrow.optimizer.SingleSourceProjectionOptimization;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.evaluation.EvaluationStrategy;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.algebra.evaluation.util.QueryOptimizerList;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailBase;

import java.io.File;

/**
 * Semagrow Sail implementation.
 * @author acharal@iit.demokritos.gr
 *
 * TODO list and other suggestions from the plenary meeting in Wageningen
 * TODO: lineage of evaluation (track the sources of the produced tuples)
 * TODO: define clean interfaces for sourceselector
 * TODO: rethink voID descriptions
 * TODO: estimate processing cost of subqueries to the sources (some sources may contain indexes etc
 * TODO: order-by and sort-merge-join
 * TODO: voID and configuration as sailbase and able to be SPARQL queried.
 * TODO: do transformation
 * TODO: geosparql
 */
public class SemagrowSail extends SailBase {


    @Override
    protected void shutDownInternal() throws SailException {

    }

    public boolean isWritable() throws SailException {
        return false;
    }

    /**
     * Creates a new Semagrow SailConnection
     * @return a new SailConnection
     * @throws SailException
     */
    @Override
    public SailConnection getConnectionInternal() throws SailException {
        return new SemagrowConnection(this);
    }

    public ValueFactory getValueFactory() {
        return ValueFactoryImpl.getInstance();
    }

    public QueryOptimizer getOptimizer() {
        SourceSelector selector = getSourceSelector();
        CostEstimator costEstimator = getCostEstimator();

        QueryOptimizerList optimizer = new QueryOptimizerList(
                new ConjunctiveConstraintSplitter(),
                new CompareOptimizer(),
                new SameTermFilterOptimizer(),
                new DynamicProgrammingOptimizer(costEstimator,selector),
                new SingleSourceProjectionOptimization()
        );

        return optimizer;
    }

    public EvaluationStrategy getEvaluationStrategy() {
        return new eu.semagrow.stack.modules.sails.semagrow.evaluation.EvaluationStrategy();
    }

    private SourceSelector getSourceSelector() {
        VOIDLoader loader = new VOIDLoader();
        ResourceSelector resourceSelector = loader.getSelector();
        return new SourceSelectorAdapter(resourceSelector);
    }

    private CostEstimator getCostEstimator() {
        CardinalityEstimator cardinalityEstimator = null;
        CostEstimator estimator = new CostEstimatorImpl(cardinalityEstimator);
        return estimator;
    }

}
