package eu.semagrow.commons.voidinfer.config;

import org.eclipse.rdf4j.sail.config.AbstractSailImplConfig;

/**
 * Created by angel on 5/30/14.
 */
public class FileReloadingMemoryStoreConfig extends AbstractSailImplConfig {

    private String filename;

    public FileReloadingMemoryStoreConfig() {
        super(FileReloadingMemoryStoreFactory.SAIL_TYPE);
    }

    public FileReloadingMemoryStoreConfig(String filename) {
        super(FileReloadingMemoryStoreFactory.SAIL_TYPE);
        this.filename = filename;
    }

    public String getFilename() { return filename; }
}
