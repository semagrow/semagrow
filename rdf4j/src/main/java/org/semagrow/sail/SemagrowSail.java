package org.semagrow.sail;

import org.semagrow.plan.QueryCompiler;
import org.semagrow.plan.QueryDecomposer;
import org.semagrow.estimator.CardinalityEstimatorResolver;
import org.semagrow.estimator.SelectivityEstimatorResolver;
import org.semagrow.estimator.CostEstimatorResolver;
import org.semagrow.evaluation.file.FileManager;
import org.semagrow.evaluation.file.MaterializationManager;
import org.semagrow.selector.RestrictiveSourceSelector;
import org.semagrow.selector.SourceSelector;
import org.semagrow.querylog.api.QueryLogException;
import org.semagrow.querylog.config.QueryLogFactory;
import org.semagrow.querylog.api.QueryLogWriter;
import org.semagrow.querylog.impl.rdf.config.RDFQueryLogConfig;
import org.semagrow.querylog.impl.rdf.config.RDFQueryLogFactory;
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

    private QueryLogWriter handler;
    private SourceSelector sourceSelector;
    private CostEstimatorResolver costEstimatorResolver;
    private CardinalityEstimatorResolver cardinalityEstimatorResolver;
    private SelectivityEstimatorResolver selectivityEstimatorResolver;
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
        CostEstimatorResolver costEstimatorResolver = getCostEstimatorResolver();
        CardinalityEstimatorResolver cardinalityEstimatorResolver = getCardinalityEstimatorResolver();
        //return new DPQueryDecomposer(costEstimatorResolver, cardinalityEstimatorResolver, selector);
        return new org.semagrow.plan.querygraph.QueryGraphDecomposer(costEstimatorResolver, cardinalityEstimatorResolver, selector);
    }

    public QueryCompiler getCompiler(Collection<IRI> includeOnly, Collection<IRI> exclude) {
        SourceSelector selector = getSourceSelector();
        selector = new RestrictiveSourceSelector(selector, includeOnly, exclude);
        CostEstimatorResolver costEstimatorResolver = getCostEstimatorResolver();
        CardinalityEstimatorResolver cardinalityEstimatorResolver = getCardinalityEstimatorResolver();
        return new org.semagrow.plan.SimpleQueryCompiler(costEstimatorResolver, cardinalityEstimatorResolver, selector);
    }

    public SourceSelector getSourceSelector() { return sourceSelector; }

    public void setSourceSelector(SourceSelector selector) {
        sourceSelector = selector;
    }

    private CostEstimatorResolver getCostEstimatorResolver() { return costEstimatorResolver; }

    public void setCostEstimatorResolver(CostEstimatorResolver costEstimatorResolver) { this.costEstimatorResolver = costEstimatorResolver; }

    private CardinalityEstimatorResolver getCardinalityEstimatorResolver() {
        return this.cardinalityEstimatorResolver;
    }

    public void setCardinalityEstimatorResolver(CardinalityEstimatorResolver cardinalityEstimatorResolver) {
        this.cardinalityEstimatorResolver = cardinalityEstimatorResolver;
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
        /*
        try {
            return factory.getQueryRecordLogger(config);
        } catch (QueryLogException e) {
            logger.warn("Cannot initialize Query Log writer", e);
        }
        */
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
