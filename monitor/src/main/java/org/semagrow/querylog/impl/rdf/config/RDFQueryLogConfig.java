package org.semagrow.querylog.impl.rdf.config;

import org.semagrow.querylog.config.QueryLogConfig;
import org.eclipse.rdf4j.rio.RDFFormat;

/**
 * Created by kzam on 5/18/15.
 */
public class RDFQueryLogConfig implements QueryLogConfig {

    private String filename;

    private int counter;

    private String logDir = "/var/tmp/log/";

    private String filePrefix = "qfr";

    private RDFFormat rdfFormat = RDFFormat.NTRIPLES;


    public String getFilename() {
        return filename;
    }


    public void setFilename(String filename) {
        this.filename = filename;
    }


    public boolean rotate() { return true; }


    public void setCounter(int counter) {
        this.counter = counter;
    }


    public int getCounter() {
        return counter;
    }

    public String getFilePrefix() {
        return filePrefix;
    }

    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    public String getLogDir() {
        return logDir;
    }

    public void setLogDir(String logDir) {
        this.logDir = logDir;
    }

    public RDFFormat getRdfFormat() {
        return rdfFormat;
    }

    public void setRdfFormat(RDFFormat rdfFormat) {
        this.rdfFormat = rdfFormat;
    }
}
