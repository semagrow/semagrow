package eu.semagrow.stack.modules.sails.config;

import org.openrdf.sail.config.SailImplConfigBase;

/**
 * Created by angel on 5/30/14.
 */
public class FileReloadingMemoryStoreConfig extends SailImplConfigBase {

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
