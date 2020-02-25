package org.semagrow.sail.config;


import org.semagrow.config.*;
import org.semagrow.connector.sparql.selector.AskSourceSelector;
import org.semagrow.estimator.*;
import org.semagrow.alignment.QueryTransformationImpl;
import org.semagrow.sail.SemagrowSail;
import org.semagrow.selector.*;
import org.semagrow.alignment.SourceSelectorWithQueryTransform;
import org.semagrow.statistics.CachedStatisticsProvider;
import org.semagrow.statistics.VOIDStatisticsProvider;
import org.semagrow.statistics.StatisticsProvider;
import org.semagrow.alignment.QueryTransformation;

import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.RepositoryConfigException;
import org.eclipse.rdf4j.repository.config.RepositoryFactory;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.config.RepositoryRegistry;
import org.eclipse.rdf4j.repository.RepositoryResolver;
import org.eclipse.rdf4j.repository.sail.config.RepositoryResolverClient;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParserRegistry;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.*;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Created by angel on 5/29/14.
 */
public class SemagrowSailFactory implements SailFactory, RepositoryResolverClient
{
    final private org.slf4j.Logger logger =
    		org.slf4j.LoggerFactory.getLogger( SemagrowSailFactory.class );

    public static final String SAIL_TYPE = "semagrow:SemagrowSail";

    private RepositoryResolver repositoryResolver;

    public String getSailType() {
        return SAIL_TYPE;
    }

    public SailImplConfig getConfig() {
        return new SemagrowSailConfig();
    }

    public Sail getSail(SailImplConfig sailImplConfig) throws SailConfigException {

        assert sailImplConfig instanceof SemagrowSailConfig;

        SemagrowSail sail = new SemagrowSail();

        try {
            SemagrowSailConfig config = (SemagrowSailConfig) sailImplConfig;

            Repository metadata = createMetadataRepository(config.getMetadataConfig());

            initializeMetadata( metadata, config.getInitialFiles() );

            sail.setMetadataRepository(metadata);

            SourceSelector selector =
                    (config.hasSelectorConfig()) ?
                            getSourceSelector(config.getSourceSelectorConfig()) :
                            getSourceSelector(metadata, config, config.getSourceSelectorConfig());

            sail.setSourceSelector(selector);

            CardinalityEstimatorResolver cardEstimator = getCardinalityEstimatorResolver(metadata, config);

            CostEstimatorResolver costEstimator = new SimpleCostEstimatorResolver(cardEstimator);

            sail.setCostEstimatorResolver(costEstimator);
            sail.setCardinalityEstimatorResolver(cardEstimator);

            sail.setBatchSize(config.getExecutorBatchSize());

            return sail;

        } catch (Exception e) {
            throw new SailConfigException(e);
        }
    }

    public Repository createMetadataRepository(RepositoryImplConfig config)
            throws RepositoryConfigException
    {
        RepositoryRegistry registry = RepositoryRegistry.getInstance();
        RepositoryFactory factory = registry.get(config.getType()).get();

        if (factory != null) {
            if (factory instanceof RepositoryResolverClient) {
                ((RepositoryResolverClient)factory).setRepositoryResolver(repositoryResolver);
            }
            return factory.getRepository(config);
        }
        else {
            throw new RepositoryConfigException("Unable to find repository factory for type " + config.getType());
        }
    }

    public SourceSelector getSourceSelector(Repository metadata, SemagrowSailConfig sailConfig, SourceSelectorImplConfig sourceSelectorImplConfig)
            throws SourceSelectorConfigException
    {

        if (sourceSelectorImplConfig instanceof RepositorySourceSelectorConfig) {

            RepositorySourceSelectorConfig config = (RepositorySourceSelectorConfig) sourceSelectorImplConfig;

            QueryTransformation transformation;

            SiteResolver siteResolver = new SimpleSiteResolver();

            SourceSelector selector = new VOIDSourceSelector(metadata, siteResolver);

            transformation = getQueryTransformation(sailConfig);

            if (transformation != null)
                selector = new SourceSelectorWithQueryTransform(selector, transformation);

            selector = new AskSourceSelector(selector);
            selector = new CachedSourceSelector(selector);

            return selector;
        }
        else
            throw new SourceSelectorConfigException();
    }

    public SourceSelector getSourceSelector(SourceSelectorImplConfig config)
            throws SourceSelectorConfigException
    {
        Optional<SourceSelectorFactory> factory = SourceSelectorRegistry.getInstance().get(config.getType());
        if (factory.isPresent()) {
            return factory.get().getSourceSelector(config);
        } else {
            throw new SourceSelectorConfigException("Cannot find appropriate source selector factory");
        }
    }

    private QueryTransformation getQueryTransformation(SemagrowSailConfig sailConfig) {

        String queryTransformationDB = sailConfig.getQueryTransformationDB();
        String queryTransformationUsername = sailConfig.getQueryTransformationUser();
        String queryTransformationPassword = sailConfig.getQueryTransformationPassword();

        if (queryTransformationDB != null)
            return new QueryTransformationImpl(queryTransformationDB, queryTransformationUsername, queryTransformationPassword);
        else
            return null;
    }

    private CardinalityEstimatorResolver getCardinalityEstimatorResolver(Repository metadata, SemagrowSailConfig config) {

        StatisticsProvider statisticsProvider = getStatisticsProvider(metadata, config);
        statisticsProvider = new CachedStatisticsProvider(statisticsProvider);
        SelectivityEstimatorResolver selectivityEstimatorResolver = new SimpleSelectivityEstimatorResolver(statisticsProvider);
        return new SimpleCardinalityEstimatorResolver(statisticsProvider, selectivityEstimatorResolver);
    }

    private StatisticsProvider getStatisticsProvider(Repository metadata, SemagrowSailConfig config) {
        return new VOIDStatisticsProvider(metadata);
    }

    public void initializeMetadata(Repository metadata, String filename)
            throws RepositoryException, IOException, RDFParseException
    {
        RepositoryConnection conn = null;

        try {
            File file = new File(filename);
            metadata.initialize();
            conn = metadata.getConnection();
            RDFFormat fileFormat = RDFFormat.matchFileName(file.getAbsolutePath(), RDFParserRegistry.getInstance().getKeys()).orElse(RDFFormat.NTRIPLES);
            conn.add(file, file.toURI().toString(), fileFormat);
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    public void initializeMetadata( Repository metadata, List<String> files )
    {
        for( String file : files ) {
            try { initializeMetadata(metadata, file); }
            catch( IOException | RDFParseException | RepositoryException ex ) {
                logger.warn( "Failed reading default metadata from file {}: ", ex.getMessage() );
            }
        }
    }

    @Override
    public void setRepositoryResolver(RepositoryResolver repositoryResolver) {
        this.repositoryResolver = repositoryResolver;
    }
}
