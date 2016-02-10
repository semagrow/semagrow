package eu.semagrow.cli;

import eu.semagrow.commons.utils.FileUtils;
import eu.semagrow.config.SemagrowRepositoryConfig;
import eu.semagrow.query.SemagrowTupleQuery;
import eu.semagrow.repository.SemagrowRepository;
import org.openrdf.model.Graph;
import org.openrdf.model.impl.GraphImpl;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQueryResultHandlerException;
import org.openrdf.query.resultio.*;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.config.*;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.StatementCollector;
import org.openrdf.sail.config.SailConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by angel on 27/11/2015.
 */
public class CliMain {

    private static final Logger logger = LoggerFactory.getLogger(CliMain.class);

    public static void main(String[] args) {

        // FIXME: argParser for the command-line arguments
        /*
           Usage: runSemagrow -c repository.ttl -q "SELECT..." -o output.json
         */
        String repositoryConfig = args[0];

        String queryString = args[1];

        String resultFile = args[2];

        logger.debug("Using configuration from {}", repositoryConfig);
        logger.debug("Writing result to file {}", resultFile);

        SemagrowRepositoryConfig repoConfig = getConfig(repositoryConfig);

        RepositoryFactory repoFactory = RepositoryRegistry.getInstance().get(repoConfig.getType());

        Repository repository = null;
        try {
            repository = (SemagrowRepository) repoFactory.getRepository(repoConfig);

            repository.initialize();
            RepositoryConnection conn = repository.getConnection();
            SemagrowTupleQuery query = (SemagrowTupleQuery) conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

            OutputStream outStream = new FileOutputStream(resultFile);

            TupleQueryResultWriter outputWriter = getWriter(resultFile);

            logger.debug("Evaluating query {}", queryString);

            query.evaluate(outputWriter);

            logger.debug("Closing connection");
            conn.close();

            logger.debug("Shuting down repository");
            repository.shutDown();

        } catch (RepositoryConfigException e) {
            e.printStackTrace();
        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (MalformedQueryException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (QueryEvaluationException e) {
            e.printStackTrace();
        } catch (TupleQueryResultHandlerException e) {
            e.printStackTrace();
        }

    }


    private static SemagrowRepositoryConfig getConfig(String repositoryFile) {

        try {
            File file = new File(repositoryFile);
            Graph configGraph = parseConfig(file);
            RepositoryConfig repConf = RepositoryConfig.create(configGraph, null);
            repConf.validate();
            RepositoryImplConfig implConf = repConf.getRepositoryImplConfig();
            return (SemagrowRepositoryConfig)implConf;
        } catch (RepositoryConfigException e) {
            e.printStackTrace();
            return new SemagrowRepositoryConfig();
        } catch (SailConfigException | IOException | NullPointerException e) {
            e.printStackTrace();
            return new SemagrowRepositoryConfig();
        }
    }

    private static TupleQueryResultWriter getWriter(String resultFile) throws FileNotFoundException {

        OutputStream outStream = new FileOutputStream(resultFile);

        TupleQueryResultFormat writerFormat = TupleQueryResultWriterRegistry.getInstance().getFileFormatForFileName(resultFile);
        TupleQueryResultWriterFactory writerFactory = TupleQueryResultWriterRegistry.getInstance().get(writerFormat);
        return writerFactory.getWriter(outStream);

    }


    protected static Graph parseConfig(File file)
            throws SailConfigException, IOException
    {
        RDFFormat format = Rio.getParserFormatForFileName(file.getAbsolutePath());
        if (format==null)
            throw new SailConfigException("Unsupported file format: " + file.getAbsolutePath());
        RDFParser parser = Rio.createParser(format);
        Graph model = new GraphImpl();
        parser.setRDFHandler(new StatementCollector(model));
        InputStream stream = new FileInputStream(file);

        try {
            parser.parse(stream, file.getAbsolutePath());
        } catch (Exception e) {
            throw new SailConfigException("Error parsing file!");
        }

        stream.close();
        return model;
    }
}
