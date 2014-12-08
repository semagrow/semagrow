package eu.semagrow.stack.modules.sails.semagrow.evaluation.file;

import info.aduna.iteration.CloseableIteration;
import org.openrdf.http.client.BackgroundTupleResult;
import org.openrdf.model.URI;
import org.openrdf.model.impl.ValueFactoryImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryResultHandler;
import org.openrdf.query.resultio.*;

import java.io.*;
import java.net.URISyntaxException;

/**
 * Created by angel on 10/20/14.
 */
public class FileManager implements MaterializationManager {

    private TupleQueryResultWriterFactory writerFactory;

    private String filePrefix = "qfr";

    private File baseDir;

    public FileManager(File baseDir, TupleQueryResultWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
        this.baseDir = baseDir;
    }

    @Override
    public CloseableIteration<BindingSet,QueryEvaluationException>
        getResult(URI q) throws QueryEvaluationException
    {
        try {
            File f = new File(convertbackURI(q));
            TupleQueryResultParserRegistry registry = TupleQueryResultParserRegistry.getInstance();
            TupleQueryResultFormat ff = registry.getFileFormatForFileName(f.getAbsolutePath());
            TupleQueryResultParserFactory factory = registry.get(ff);
            TupleQueryResultParser parser = factory.getParser();
            InputStream in = new FileInputStream(f);
            return new BackgroundTupleResult(parser, in, null);
            //return new BackgroundTupleResult(parser, in);
        } catch (URISyntaxException | FileNotFoundException e) {
            throw new QueryEvaluationException(e);
        }
    }

    @Override
    public StoreHandler saveResult()  throws QueryEvaluationException {

        try {
            File file = getNewFile();
            URI storeId = convertURI(file.toURI());
            OutputStream out = new FileOutputStream(file);
            TupleQueryResultWriter writer = writerFactory.getWriter(out);
            return new StoreHandler(storeId, writer);
        } catch (IOException e) {
            throw new QueryEvaluationException(e);
        }
    }

    private File getNewFile() throws IOException
    {
        String ext = writerFactory.getTupleQueryResultFormat().getDefaultFileExtension();
        return File.createTempFile(filePrefix, "." + ext, baseDir);
    }

    public static URI convertURI(java.net.URI uri) {
        return ValueFactoryImpl.getInstance().createURI(uri.toString());
    }

    public static java.net.URI convertbackURI(URI uri) throws URISyntaxException {
        return new java.net.URI(uri.stringValue());
    }

    protected class StoreHandler
            extends QueryResultHandlerWrapper
            implements MaterializationHandle
    {
        private URI id;

        public StoreHandler(URI id, QueryResultHandler handler) {
            super(handler);
            this.id = id;
        }

        public URI getId() { return id; }

        public void handleException(Exception e) { }

        public void destroy() { }
    }


}
