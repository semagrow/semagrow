package org.semagrow.cli;

import org.eclipse.rdf4j.query.*;
import org.eclipse.rdf4j.repository.sail.config.RepositoryResolver;
import org.semagrow.repository.SemagrowRepositoryResolver;
import org.eclipse.rdf4j.query.resultio.*;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Created by angel on 27/11/2015.
 */
public class CliMain {

    private static final Logger logger = LoggerFactory.getLogger(CliMain.class);

    private static RepositoryResolver resolver = new SemagrowRepositoryResolver();

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

        Repository repository = null;
        try {
            repository = resolver.getRepository(null);

            repository.initialize();
            RepositoryConnection conn = repository.getConnection();
            TupleQuery query = (TupleQuery) conn.prepareTupleQuery(QueryLanguage.SPARQL, queryString);

            TupleQueryResultWriter outputWriter = getWriter(resultFile);

            logger.debug("Evaluating query {}", queryString);

            query.evaluate(outputWriter);

            logger.debug("Closing connection");
            conn.close();

            logger.debug("Shutting down repository");
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

    private static TupleQueryResultWriter getWriter(String resultFile) throws FileNotFoundException {

        OutputStream outStream = new FileOutputStream(resultFile);

        QueryResultFormat writerFormat = TupleQueryResultWriterRegistry.getInstance().getFileFormatForFileName(resultFile).get();
        TupleQueryResultWriterFactory writerFactory = TupleQueryResultWriterRegistry.getInstance().get(writerFormat).get();
        return writerFactory.getWriter(outStream);

    }
}
