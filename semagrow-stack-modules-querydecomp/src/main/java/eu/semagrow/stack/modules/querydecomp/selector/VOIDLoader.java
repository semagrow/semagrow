package eu.semagrow.stack.modules.querydecomp.selector;

import org.apache.commons.io.filefilter.NameFileFilter;
import org.apache.commons.io.monitor.FileAlterationListenerAdaptor;
import org.apache.commons.io.monitor.FileAlterationMonitor;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.openrdf.model.*;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.RepositoryResult;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFParseException;
import org.openrdf.sail.SailException;
import org.openrdf.sail.memory.MemoryStore;

import java.io.*;

/**
 * Created by angel on 3/18/14.
 */
public class VOIDLoader {

    private static final String VOIDNamespace = "http://rdfs.org/ns/void#";

    private static final String RDFNamespace = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";

    private static final String RDFSNamespace = "http://www.w3.org/2000/01/rdf-schema#";

    private static final String OWLNamespace = "http://www.w3.org/2002/07/owl#";

    private static final ValueFactory factory = ValueFactoryImpl.getInstance();

    private static final URI voidsubset = factory.createURI(VOIDNamespace,"subset");

    private static final URI voiddataset = factory.createURI(VOIDNamespace,"Dataset");

    private static final URI apred = factory.createURI(RDFNamespace,"a");

    private static final URI rdftype = factory.createURI(RDFNamespace,"type");

    private static final URI rdfssubClassOf = factory.createURI(RDFSNamespace,"subClassOf");

    private static final URI owlClass = factory.createURI(OWLNamespace,"Class");

    private static final String VOIDFilename = "/home/angel/Downloads/demo.rdf";

    protected class ReloadListener extends FileAlterationListenerAdaptor {

        private VOIDResourceSelector selector;

        public ReloadListener(VOIDResourceSelector selector) {
            this.selector = selector;
        }

        @Override
        public void onFileChange(File file) {
            //selector.reset();
            //selector.loadRDFFile(file.getAbsolutePath());
            System.out.print("file " + file.getAbsolutePath() + "changed!");
        }
    }

    private FileAlterationMonitor monitor = null;

    private FileAlterationMonitor getMonitor() {
        if (monitor == null)
            monitor = new FileAlterationMonitor(5);
        return monitor;
    }

    public void attachReload(String filename) throws Exception {
        File file = new File(filename);
        if (file.isFile()) {
            FileFilter filter = new NameFileFilter(file.getName());
            FileAlterationObserver observer = new FileAlterationObserver(file.getParentFile().getAbsolutePath(), filter);
            observer.addListener(new ReloadListener(selector));
            FileAlterationMonitor monitor = getMonitor();
            monitor.addObserver(observer);
            monitor.start();
        }
    }

    protected Repository loadFromFile(String filename) throws RepositoryException, IOException, RDFParseException, SailException {

        Repository repository = new SailRepository(new MemoryStore());
        repository.initialize();
        RepositoryConnection connection = repository.getConnection();

        Repository finalRepository = getRepository();
        RepositoryConnection conn = finalRepository.getConnection();

        try {
            InputStream stream = new FileInputStream(filename);
            connection.add(stream, "file://" + filename, RDFFormat.RDFXML);
            RepositoryResult<Statement> statements = connection.getStatements(null,null,null,true);

            while (statements.hasNext()) {
                Statement fixed = fixStatement(statements.next());
                conn.add(fixed);
            }
        }
        catch (IOException e) {
            throw e;
        }
        catch (RDFParseException e) {
            throw e;
        }
        finally
        {
            connection.close();
            conn.close();
            repository.shutDown();
        }

        return finalRepository;
    }

    private Statement fixStatement(Statement statement) {

        Resource sub = statement.getSubject();
        Value obj = statement.getObject();

        if (sub instanceof URI)
            sub = fixURI((URI)statement.getSubject());
        URI pred = fixURI(statement.getPredicate());

        if (obj instanceof URI)
            obj = fixURI((URI) statement.getObject());

        if (pred.equals(rdfssubClassOf))
            pred = voidsubset;

        if (pred.equals(rdftype) && obj.equals(owlClass)) {
            pred = apred;
            obj = voiddataset;
        }

        return factory.createStatement(sub,pred,obj);
        //return statement;
    }

    private URI fixURI(URI uri) {

        if (uri.getNamespace().equals("void:"))
            return factory.createURI(VOIDNamespace, uri.getLocalName());
        else
            return uri;
    }

    private Repository getRepository() throws SailException {
        MemoryStore store = new MemoryStore();
        store.initialize();
        return new SailRepository(store);
    }

    private VOIDResourceSelector selector = null;

    public VOIDResourceSelector getSelector() {
        try {
            if (selector == null) {
                Repository repository = loadFromFile(VOIDFilename);
                selector = new VOIDResourceSelector();
                selector.setRepository(repository);
                attachReload(VOIDFilename);
            }
            return selector;
        } catch (RepositoryException e) {
            e.printStackTrace();
        } catch (SailException e) {
            e.printStackTrace();
        } catch (RDFParseException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}
