package org.semagrow.repository;

import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.BooleanQueryResultParserRegistry;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultFormat;
import org.eclipse.rdf4j.query.resultio.TupleQueryResultParserRegistry;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.config.*;
import org.eclipse.rdf4j.repository.sail.config.RepositoryResolver;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.semagrow.repository.config.SemagrowRepositoryConfig;
import org.semagrow.util.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Created by angel on 17/6/2016.
 */
public class SemagrowRepositoryResolver implements RepositoryResolver {

    @Override
    public Repository getRepository(String s)
            throws RepositoryException, RepositoryConfigException
    {
        RepositoryImplConfig repoConfig = getConfig();
        RepositoryFactory repoFactory = RepositoryRegistry.getInstance().get(repoConfig.getType()).get();
        Repository repository = repoFactory.getRepository(repoConfig);
        repository.initialize();

        // remove CSV and TSV format due to bug: literals are recognized as URIs if they contain a substring parsable as URI.
        TupleQueryResultParserRegistry registry = TupleQueryResultParserRegistry.getInstance();

        registry.get(TupleQueryResultFormat.CSV).ifPresent(f -> registry.remove(f));
        registry.get(TupleQueryResultFormat.TSV).ifPresent(f -> registry.remove(f));
        registry.get(TupleQueryResultFormat.JSON).ifPresent(f -> registry.remove(f));

        BooleanQueryResultParserRegistry booleanRegistry = BooleanQueryResultParserRegistry.getInstance();
        booleanRegistry.get(BooleanQueryResultFormat.JSON).ifPresent(f -> booleanRegistry.remove(f));

        return repository;
    }

    protected RepositoryImplConfig getConfig() {

        try {
            File file = FileUtils.getFile("repository.ttl");
            Model configGraph = parseConfig(file);
            RepositoryConfig repConf = RepositoryConfig.create(configGraph, null);
            repConf.validate();
            return repConf.getRepositoryImplConfig();
        } catch (RepositoryConfigException e) {
            e.printStackTrace();
            return new SemagrowRepositoryConfig();
        } catch (SailConfigException | IOException | NullPointerException e) {
            e.printStackTrace();
            return new SemagrowRepositoryConfig();
        }
    }

    private Model parseConfig(File file) throws SailConfigException, IOException
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
