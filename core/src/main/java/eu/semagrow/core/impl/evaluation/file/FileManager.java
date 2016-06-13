package eu.semagrow.core.impl.evaluation.file;

import org.eclipse.rdf4j.common.iteration.CloseableIteration;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.QueryResultHandler;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.resultio.*;

import java.io.*;
import java.net.URISyntaxException;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 * Created by angel on 10/20/14.
 */
public class FileManager implements MaterializationManager {

    private TupleQueryResultWriterFactory writerFactory;

    private String filePrefix = "result";

    private File baseDir;

    private ExecutorService executor;

    public FileManager(File baseDir, TupleQueryResultWriterFactory writerFactory, ExecutorService executorService) {
        this.writerFactory = writerFactory;
        this.baseDir = baseDir;
        this.executor = executorService;
    }

    public FileManager(File baseDir, TupleQueryResultWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
        this.baseDir = baseDir;
    }


    @Override
    public CloseableIteration<BindingSet,QueryEvaluationException>
        getResult(IRI q) throws QueryEvaluationException
    {
        try {
            File f = new File(convertbackURI(q));
            TupleQueryResultParserRegistry registry = TupleQueryResultParserRegistry.getInstance();
            QueryResultFormat ff = registry.getFileFormatForFileName(f.getAbsolutePath()).get();
            TupleQueryResultParserFactory factory = registry.get(ff).get();
            TupleQueryResultParser parser = factory.getParser();
            InputStream in = new FileInputStream(f);
            org.eclipse.rdf4j.http.client.BackgroundTupleResult result = new org.eclipse.rdf4j.http.client.BackgroundTupleResult(parser, in);

           // execute(result);
            return result;
        } catch (URISyntaxException | FileNotFoundException e) {
            throw new QueryEvaluationException(e);
        }
    }

    @Override
    public StoreHandler saveResult()  throws QueryEvaluationException {

        try {
            File file = getNewFile();
            IRI storeId = convertURI(file.toURI());
            OutputStream out = new FileOutputStream(file, true);
            TupleQueryResultWriter writer = writerFactory.getWriter(out);
            return new StoreHandler(storeId, writer, out);
        } catch (IOException e) {
            throw new QueryEvaluationException(e);
        }
    }

    public void execute(Runnable runnable) {
        executor.execute(runnable);
    }

    private File getNewFile() throws IOException
    {
        String ext = writerFactory.getTupleQueryResultFormat().getDefaultFileExtension();
        return File.createTempFile(filePrefix, "." + ext, baseDir);
    }

    public static IRI convertURI(java.net.URI uri) {
        return SimpleValueFactory.getInstance().createIRI(uri.toString());
    }

    public static java.net.URI convertbackURI(IRI uri) throws URISyntaxException {
        return new java.net.URI(uri.stringValue());
    }

    protected class StoreHandler
            extends QueryResultHandlerWrapper
            implements MaterializationHandle
    {
        private IRI id;
        private boolean started = false;
        private OutputStream out;

        public StoreHandler(IRI id, QueryResultHandler handler, OutputStream out) {
            super(handler);
            this.id = id;
            this.out = out;
        }

        public IRI getId() { return id; }

        public void handleException(Exception e) {
            try {
                this.destroy();
            } catch (IOException e1) {
                e1.printStackTrace();
            }
        }

        @Override
        public void startQueryResult(List<String> bindingNames) throws TupleQueryResultHandlerException {
            super.startQueryResult(bindingNames);
            started = true;
        }

        @Override
        public void endQueryResult() throws TupleQueryResultHandlerException {
            if (started) {
                super.endQueryResult();
            }

            try {
                out.close();
            }catch(IOException e) {
                throw new TupleQueryResultHandlerException(e);
            }
        }

        public void destroy() throws IOException {

            try {

                endQueryResult();

                File f = null;
                f = new File(convertbackURI(id));
                f.delete();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }


}
