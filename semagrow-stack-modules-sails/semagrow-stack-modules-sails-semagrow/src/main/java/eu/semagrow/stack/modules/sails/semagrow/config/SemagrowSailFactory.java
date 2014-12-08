package eu.semagrow.stack.modules.sails.semagrow.config;

import eu.semagrow.stack.modules.alignment.QueryTransformationImpl;
import eu.semagrow.stack.modules.api.estimator.CardinalityEstimator;
import eu.semagrow.stack.modules.api.estimator.CostEstimator;
import eu.semagrow.stack.modules.api.source.SourceSelector;
import eu.semagrow.stack.modules.api.statistics.Statistics;
import eu.semagrow.stack.modules.api.transformation.QueryTransformation;
import eu.semagrow.stack.modules.querydecomp.selector.SourceSelectorWithQueryTransform;
import eu.semagrow.stack.modules.querydecomp.selector.VOIDSourceSelector;
import eu.semagrow.stack.modules.querydecomp.selector.VOIDStatistics;
import eu.semagrow.stack.modules.sails.semagrow.SemagrowSail;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CardinalityEstimatorImpl;
import eu.semagrow.stack.modules.sails.semagrow.estimator.CostEstimatorImpl;
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

            SourceSelector selector = getSourceSelector(metadata, config, config.getSourceSelectorConfig());

            sail.setSourceSelector(selector);

            CardinalityEstimator cardEstimator = getCardinalityEstimator(metadata, config);

            CostEstimator costEstimator = new CostEstimatorImpl(cardEstimator);

            sail.setCostEstimator(costEstimator);
            sail.setCardinalityEstimator(cardEstimator);

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

            return selector;
        }
        else
            throw new SourceSelectorConfigException();
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

        Statistics statistics = getStatistics(metadata, config);
        return new CardinalityEstimatorImpl(statistics);
    }

    private Statistics getStatistics(Repository metadata, SemagrowSailConfig config) {
        return new VOIDStatistics(metadata);
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
