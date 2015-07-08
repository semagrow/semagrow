package eu.semagrow.sail;

import eu.semagrow.core.decomposer.QueryDecomposer;
import eu.semagrow.core.estimator.CardinalityEstimator;
import eu.semagrow.core.evaluation.FederatedQueryEvaluation;
import eu.semagrow.core.impl.estimator.CostEstimator;
import eu.semagrow.core.impl.evaluation.QueryEvaluationImpl;
import eu.semagrow.core.impl.evaluation.file.FileManager;
import eu.semagrow.core.impl.evaluation.file.MaterializationManager;
import eu.semagrow.core.impl.planner.DPQueryDecomposer;
import eu.semagrow.core.impl.selector.RestrictiveSourceSelector;
import eu.semagrow.core.source.SourceSelector;
import eu.semagrow.querylog.QueryLogManager;
import eu.semagrow.querylog.api.QueryLogException;
import eu.semagrow.querylog.api.QueryLogFactory;
import eu.semagrow.querylog.api.QueryLogWriter;
import eu.semagrow.querylog.config.FileQueryLogConfig;
import eu.semagrow.querylog.rdf.RDFQueryLogFactory;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.algebra.evaluation.QueryOptimizer;
import org.openrdf.query.algebra.evaluation.impl.CompareOptimizer;
import org.openrdf.query.algebra.evaluation.impl.ConjunctiveConstraintSplitter;
import org.openrdf.query.algebra.evaluation.impl.SameTermFilterOptimizer;
import org.openrdf.query.algebra.evaluation.util.QueryOptimizerList;
import org.openrdf.query.resultio.TupleQueryResultFormat;
import org.openrdf.query.resultio.TupleQueryResultWriterFactory;
import org.openrdf.query.resultio.TupleQueryResultWriterRegistry;
import org.openrdf.repository.Repository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFWriterFactory;
import org.openrdf.rio.RDFWriterRegistry;
import org.openrdf.sail.SailConnection;
import org.openrdf.sail.SailException;
import org.openrdf.sail.helpers.SailBase;

import java.io.*;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Semagrow Sail implementation.
 * @author acharal@iit.demokritos.gr
 *
 * TODO list and other suggestions from the plenary meeting in Wageningen
 * TODO: define clean interfaces for sourceselector
 * TODO: rethink voID descriptions
 * TODO: estimate processing cost of subqueries to the sources (some sources may contain indexes etc
 * TODO: voID and configuration as sailbase and able to be SPARQL queried.
 * TODO: do transformation
 * TODO: geosparql
 */
public class SemagrowSail extends SailBase {

    private FederatedQueryEvaluation queryEvaluation;
    private final static String logDir = "/var/tmp/log/";

    private QueryLogWriter handler;
    private final static String filePrefix = "qfr";
    
    private SourceSelector sourceSelector;
    private CostEstimator costEstimator;
    private CardinalityEstimator cardinalityEstimator;

    private int batchSize;

    private ExecutorService executor = Executors.newCachedThreadPool();
    private Repository metadataRepository;

    public SemagrowSail() { }

    public boolean isWritable() throws SailException {
        return false;
    }

    public ValueFactory getValueFactory() {
        return ValueFactoryImpl.getInstance();
    }

    public SailConnection getConnectionInternal() throws SailException {
        return new SemagrowSailConnection(this);
    }

    public QueryOptimizer getOptimizer() {
        SourceSelector selector = getSourceSelector();
        CostEstimator costEstimator = getCostEstimator();

        QueryOptimizerList optimizer = new QueryOptimizerList(
                new ConjunctiveConstraintSplitter(),
                new CompareOptimizer(),
                new SameTermFilterOptimizer()
        );

        return optimizer;
    }

    public QueryDecomposer getDecomposer(Collection<URI> includeOnly, Collection<URI> exclude) {
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
            MaterializationManager manager = getManager();
            handler = getRecordLog();
            queryEvaluation = new QueryEvaluationImpl(manager, handler, executor);
        }

        return queryEvaluation;
    }

    public void setQueryEvaluation(FederatedQueryEvaluation queryEvaluation) {
        this.queryEvaluation = queryEvaluation;
    }



    public MaterializationManager getManager() {
        File baseDir = new File(logDir);
        TupleQueryResultFormat resultFF = TupleQueryResultFormat.TSV;

        TupleQueryResultWriterRegistry  registry = TupleQueryResultWriterRegistry.getInstance();
        TupleQueryResultWriterFactory writerFactory = registry.get(resultFF);
        MaterializationManager manager = new FileManager(baseDir, writerFactory);

        return manager;
    }

    public QueryLogWriter getRecordLog() {

        QueryLogWriter handler;

        FileQueryLogConfig config = new FileQueryLogConfig();
        QueryLogManager qfrManager = new QueryLogManager(logDir, filePrefix);
        try {
            config.setFilename(qfrManager.getLastFile());
            config.setCounter(3);
        } catch (QueryLogException e) {
            e.printStackTrace();
        }

        RDFFormat rdfFF = RDFFormat.NTRIPLES;

        RDFWriterRegistry writerRegistry = RDFWriterRegistry.getInstance();
        RDFWriterFactory rdfWriterFactory = writerRegistry.get(rdfFF);
        QueryLogFactory factory = new RDFQueryLogFactory(rdfWriterFactory);

        try {
            handler = factory.getQueryRecordLogger((FileQueryLogConfig) config);
            return handler;
        } catch (QueryLogException e) {
            e.printStackTrace();
        }
        return null;
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