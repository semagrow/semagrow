package eu.semagrow.cli;

import eu.semagrow.config.SemagrowRepositoryConfig;
import eu.semagrow.query.SemagrowTupleQuery;
import eu.semagrow.repository.SemagrowRepository;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.query.MalformedQueryException;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryLanguage;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.resultio.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.*;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.sail.config.SailConfigException;
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

        RepositoryFactory repoFactory = RepositoryRegistry.getInstance().get(repoConfig.getType()).get();

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
            Model configGraph = parseConfig(file);
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

        QueryResultFormat writerFormat = TupleQueryResultWriterRegistry.getInstance().getFileFormatForFileName(resultFile).get();
        TupleQueryResultWriterFactory writerFactory = TupleQueryResultWriterRegistry.getInstance().get(writerFormat).get();
        return writerFactory.getWriter(outStream);

    }


    protected static Model parseConfig(File file)
            throws SailConfigException, IOException
    {
        RDFFormat format = Rio.getParserFormatForFileName(file.getAbsolutePath()).get();
        if (format==null)
            throw new SailConfigException("Unsupported file format: " + file.getAbsolutePath());
        RDFParser parser = Rio.createParser(format);
        Model model = new LinkedHashModel();
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
