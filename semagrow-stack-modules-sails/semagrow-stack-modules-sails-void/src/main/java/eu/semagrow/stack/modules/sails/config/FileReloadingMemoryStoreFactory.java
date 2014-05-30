package eu.semagrow.stack.modules.sails.config;

import eu.semagrow.stack.modules.sails.memory.FileReloadingMemoryStore;
import org.openrdf.sail.Sail;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailFactory;
import org.openrdf.sail.config.SailImplConfig;

/**
 * Created by angel on 5/30/14.
 */
public class FileReloadingMemoryStoreFactory implements SailFactory {

    public static final String SAIL_TYPE = "semagrow:FileReloadingMemoryStore";

    public String getSailType() { return SAIL_TYPE; }

    public SailImplConfig getConfig() {
        return new FileReloadingMemoryStoreConfig();
    }

    public Sail getSail(SailImplConfig sailImplConfig) throws SailConfigException {
        assert sailImplConfig instanceof FileReloadingMemoryStoreConfig;
        String filename = ((FileReloadingMemoryStoreConfig)sailImplConfig).getFilename();
        return new FileReloadingMemoryStore(filename);
    }
}
