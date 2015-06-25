package eu.semagrow.querylog.rdf;

import eu.semagrow.querylog.api.*;
import eu.semagrow.querylog.config.FileQueryLogConfig;
import org.openrdf.rio.RDFWriter;
import org.openrdf.rio.RDFWriterFactory;

import java.io.*;

/**
 * Created by angel on 10/21/14.
 */
public class RDFQueryLogFactory implements QueryLogFactory {

    private RDFWriterFactory writerFactory;
    private FileQueryLogConfig config = null;


    public RDFQueryLogFactory(RDFWriterFactory writerFactory) {
        this.writerFactory = writerFactory;
    }

    @Override
    public QueryLogWriter getQueryRecordLogger(QueryLogConfig config) throws QueryLogException {

        if (config instanceof FileQueryLogConfig) {
            this.config = (FileQueryLogConfig) config;


            if (this.config.rotate()) {
                try {
                    QueryLogWriter handler = new RotatingQueryLogWriter(this.config);

                    return handler;
                } catch (QueryLogException e) {
                    throw new QueryLogException(e);
                }
            } else {
                try {
                    return getQueryRecordLogger(this.config.getFilename());
                } catch (FileNotFoundException e) {
                    throw new QueryLogException(e);
                }

            }
        }
        throw new QueryLogException("Wrong query log config instance");

    }

    public QueryLogWriter getQueryRecordLogger(OutputStream out) {

        RDFWriter writer = writerFactory.getWriter(out);

        QueryLogWriter handler = new RDFQueryLogWriter(writer);

        try {
            handler.startQueryLog();
        } catch (QueryLogException e) {
            e.printStackTrace();
        }
        return handler;
    }

    public QueryLogWriter getQueryRecordLogger(String out) throws FileNotFoundException {

        FileOutputStream fileStream = new FileOutputStream(out, true);

        return getQueryRecordLogger(fileStream);
    }


    /////
    private class RotatingQueryLogWriter implements QueryLogWriter
    {
        private FileQueryLogConfig config;
        private QueryLogWriter actualWriter;
        private int counter = 0;
        private long rotation = 0;

        public RotatingQueryLogWriter(FileQueryLogConfig config) throws QueryLogException {
            this.config = config;

            getLastRotation();
            actualWriter = newWriter();

        }

        @Override
        public void startQueryLog() throws QueryLogException {
            actualWriter.startQueryLog();
        }

        @Override
        public void endQueryLog() throws QueryLogException {
            actualWriter.endQueryLog();
        }

        @Override
        public void handleQueryRecord(QueryLogRecord queryLogRecord) throws QueryLogException {
            actualWriter.handleQueryRecord(queryLogRecord);

            counter++;
            checkRotate();
        }

        private void checkRotate() throws QueryLogException {
            boolean mustRotate = false;

            mustRotate = counter > this.config.getCounter();

            if (mustRotate) {
                actualWriter.endQueryLog();
                initWriter();
            }
        }

        private void initWriter() throws QueryLogException {
            computeRotation();
            actualWriter = newWriter();
            counter = 0;
        }

        private void computeRotation() {
            if(rotation == Long.MAX_VALUE) {
                rotation = 0;
            }
            else { rotation++; }
        }

        private QueryLogWriter newWriter() throws QueryLogException
        {
            String filename = computeNextFilename();

            try {
                QueryLogWriter handler = getQueryRecordLogger(filename);
                return handler;
            }
            catch (FileNotFoundException e) {
                throw new QueryLogException(e);
            }
        }

        private void getLastRotation() throws QueryLogException {
            try {
                rotation = Long.parseLong(getFilenamePart(1));

                computeRotation();
            } catch (Exception e) {
                throw new QueryLogException(e);
            }
        }

        private String getFilenamePart(int part) {
            String[] splitArray = config.getFilename().split("\\.");
            return splitArray[part];
        }

        private String computeNextFilename() throws QueryLogException {
            if(isEmptyFile())
                return config.getFilename();

            String filename = getFilenamePart(0) + "." + rotation;
            config.setFilename(filename);

            return createFile(filename);
        }

        private boolean isEmptyFile() {
            File file = new File(config.getFilename());

            if(file.length() == 0)
                return true;
            return false;
        }

        private String createFile(String filename)  throws QueryLogException {
            File file = new File(filename);

            try {
                if(file.createNewFile()) {
                    return filename;
                }
            } catch (IOException e) {
                throw new QueryLogException(e);
            }

            throw new QueryLogException("Error in creating new qfr file");
        }
    }

}
