package eu.semagrow.sail;

import eu.semagrow.core.plan.QueryDecomposer;
import eu.semagrow.core.estimator.CardinalityEstimator;
import eu.semagrow.core.evalit.FederatedQueryEvaluation;
import eu.semagrow.core.estimator.CostEstimator;
import eu.semagrow.core.impl.evalit.QueryEvaluationImpl;
import eu.semagrow.core.impl.evaluation.file.FileManager;
import eu.semagrow.core.impl.evaluation.file.MaterializationManager;
import eu.semagrow.core.impl.plan.DPQueryDecomposer;
import eu.semagrow.core.impl.selector.RestrictiveSourceSelector;
import eu.semagrow.core.source.SourceSelector;
import eu.semagrow.querylog.api.QueryLogException;
import eu.semagrow.querylog.config.QueryLogFactory;
import eu.semagrow.querylog.api.QueryLogWriter;
import eu.semagrow.querylog.impl.rdf.config.RDFQueryLogConfig;
import eu.semagrow.querylog.impl.rdf.config.RDFQueryLogFactory;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.algebra.evaluation.QueryOptimizer;
import org.eclipse.rdf4j.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.eclipse.rdf4j.query.algebra.evaluation.util.QueryOptimizerList;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterFactory;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultWriterRegistry;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.sail.SailConnection;
import org.eclipse.rdf4j.sail.SailException;
import org.eclipse.rdf4j.sail.helpers.AbstractSail;

import java.io.*;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Semagrow Sail implementation.
 * @author acharal@iit.demokritos.gr
 *
 * TODO list and other suggestions from the plenary meeting in Wageningen
 * TODO: estimate processing cost of subqueries to the sources (some sources may contain indexes etc
 * TODO: geosparql
 */
public class SemagrowSail extends AbstractSail {

    private FederatedQueryEvaluation queryEvaluation;
    private QueryLogWriter handler;
    private SourceSelector sourceSelector;
    private CostEstimator costEstimator;
    private CardinalityEstimator cardinalityEstimator;
    private MaterializationManager materializationManager;

    private int batchSize;

    private ExecutorService executor = Executors.newCachedThreadPool();
    private Repository metadataRepository;

    public SemagrowSail() {
        handler = createRecordLog();
    }

    public boolean isWritable() throws SailException {
        return false;
    }

    public ValueFactory getValueFactory() {
        return SimpleValueFactory.getInstance();
    }

    public SailConnection getConnectionInternal() throws SailException {
        return new SemagrowSailConnection(this);
    }

    public QueryOptimizer getOptimizer() {
        SourceSelector selector = getSourceSelector();
        CostEstimator costEstimator = getCostEstimator();

        QueryOptimizerList optimizer = new QueryOptimizerList(
                new ConjunctiveConstraintSplitter()//,
                //new CompareOptimizer(),
                //new SameTermFilterOptimizer(),
                //new FilterOptimizer()
        );

        return optimizer;
    }

    public QueryDecomposer getDecomposer(Collection<IRI> includeOnly, Collection<IRI> exclude) {
        SourceSelector selector = getSourceSelector();
        selector = new RestrictiveSourceSelector(selector, includeOnly, exclude);
        CostEstimator costEstimator = getCostEstimator();
        CardinalityEstimator cardinalityEstimator = getCardinalityEstimator();
        return new DPQueryDecomposer(costEstimator, cardinalityEstimator, selector);
    }

    public SourceSelector getSourceSelector() { return sourceSelector; }

    public void setSourceSelector(SourceSelector selector) {
        sourceSelector = selector;
    }

    private CostEstimator getCostEstimator() { return costEstimator; }

    public void setCostEstimator(CostEstimator costEstimator) { this.costEstimator = costEstimator; }

    private CardinalityEstimator getCardinalityEstimator() {
        return this.cardinalityEstimator;
    }

    public void setCardinalityEstimator(CardinalityEstimator cardinalityEstimator) {
        this.cardinalityEstimator = cardinalityEstimator;
    }

    public FederatedQueryEvaluation getQueryEvaluation() {

        if (queryEvaluation == null) {
            //handler = getRecordLog();
            queryEvaluation = new QueryEvaluationImpl(getManager(), handler, executor);
        }
        return queryEvaluation;
    }

    public void setQueryEvaluation(FederatedQueryEvaluation queryEvaluation) {
        this.queryEvaluation = queryEvaluation;
    }

    public MaterializationManager getManager() {
        return materializationManager;
    }

    private QueryLogWriter createRecordLog() {

        RDFQueryLogConfig config = new RDFQueryLogConfig();
        QueryLogFactory factory = new RDFQueryLogFactory();

        config.setCounter(3);
        config.setRdfFormat(RDFFormat.NTRIPLES);

        File baseDir = new File(config.getLogDir());
        TupleQueryResultFormat resultFF = TupleQueryResultFormat.TSV;

        TupleQueryResultWriterRegistry  registry = TupleQueryResultWriterRegistry.getInstance();
        TupleQueryResultWriterFactory writerFactory = registry.get(resultFF).get();
        materializationManager = new FileManager(baseDir, writerFactory);

        try {
            return factory.getQueryRecordLogger(config);
        } catch (QueryLogException e) {
            logger.warn("Cannot initialize Query Log writer", e);
        }
        return null;
    }

    public QueryLogWriter getRecordLog() {
        return handler;
    }

    @Override
    public void shutDownInternal() throws SailException {


        if (handler != null) {
            try {
                handler.endQueryLog();
            } catch (QueryLogException e) {
                throw new SailException(e);
            }
        }
       // super.shutDown();

    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int b) {
        batchSize = b;
    }

    public Repository getMetadataRepository() {
        return metadataRepository;
    }

    public void setMetadataRepository(Repository metadataRepository) { this.metadataRepository = metadataRepository; }

}
