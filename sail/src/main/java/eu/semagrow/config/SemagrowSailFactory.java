package eu.semagrow.config;


import eu.semagrow.core.config.SourceSelectorConfigException;
import eu.semagrow.core.config.SourceSelectorFactory;
import eu.semagrow.core.config.SourceSelectorImplConfig;
import eu.semagrow.core.estimator.CardinalityEstimator;
import eu.semagrow.core.impl.alignment.QueryTransformationImpl;
import eu.semagrow.core.impl.estimator.CostEstimator;
import eu.semagrow.core.impl.selector.*;
import eu.semagrow.core.impl.statistics.CachedStatisticsProvider;
import eu.semagrow.core.impl.statistics.VOIDStatisticsProvider;
import eu.semagrow.core.source.SourceSelector;
import eu.semagrow.core.statistics.StatisticsProvider;
import eu.semagrow.core.transformation.QueryTransformation;
import eu.semagrow.sail.SemagrowSail;
import eu.semagrow.core.impl.estimator.CardinalityEstimatorImpl;
import eu.semagrow.core.impl.estimator.CostEstimatorImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.RepositoryConfigException;
import org.openrdf.repository.config.RepositoryFactory;
import org.openrdf.repository.config.RepositoryImplConfig;
import org.openrdf.repository.config.RepositoryRegistry;
import org.openrdf.repository.sail.config.RepositoryResolver;
import org.openrdf.repository.sail.config.RepositoryResolverClient;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.Sail;
import org.openrdf.sail.config.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by angel on 5/29/14.
 */
public class SemagrowSailFactory implements SailFactory, RepositoryResolverClient {

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

            List<String> files = config.getInitialFiles();
            for (String file : files) {
                initializeMetadata(metadata, file);
            }

            sail.setMetadataRepository(metadata);

            SourceSelector selector =
                    (config.hasSelectorConfig()) ?
                            getSourceSelector(config.getSourceSelectorConfig()) :
                            getSourceSelector(metadata, config, config.getSourceSelectorConfig());

            sail.setSourceSelector(selector);

            CardinalityEstimator cardEstimator = getCardinalityEstimator(metadata, config);

            CostEstimator costEstimator = new CostEstimatorImpl(cardEstimator);

            sail.setCostEstimator(costEstimator);
            sail.setCardinalityEstimator(cardEstimator);

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
        RepositoryFactory factory = registry.get(config.getType());

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

            SourceSelector selector = new VOIDSourceSelector(metadata);

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
        SourceSelectorFactory factory = SourceSelectorRegistry.getInstance().get(config.getType());
        if (factory != null) {
            return factory.getSourceSelector(config);
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

    private CardinalityEstimator getCardinalityEstimator(Repository metadata, SemagrowSailConfig config) {

        StatisticsProvider statistics = getStatistics(metadata, config);
        statistics = new CachedStatisticsProvider(statistics);
        return new CardinalityEstimatorImpl(statistics);
    }

    private StatisticsProvider getStatistics(Repository metadata, SemagrowSailConfig config) {
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
            RDFFormat fileFormat = RDFFormat.forFileName(file.getAbsolutePath(), RDFFormat.NTRIPLES);
            conn.add(file, file.toURI().toString(), fileFormat);
        } finally {
            if (conn != null)
                conn.close();
        }
    }

    public void initializeMetadata(Repository metadata, List<String> files) {

        for (String file : files) {
            try {
                initializeMetadata(metadata, file);
            } catch (IOException | RDFParseException | RepositoryException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void setRepositoryResolver(RepositoryResolver repositoryResolver) {
        this.repositoryResolver = repositoryResolver;
    }

}
