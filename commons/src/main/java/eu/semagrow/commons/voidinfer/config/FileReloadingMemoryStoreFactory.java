package eu.semagrow.commons.voidinfer.config;

import eu.semagrow.commons.voidinfer.memory.FileReloadingMemoryStore;
import eu.semagrow.commons.voidinfer.memory.FileReloadingMemoryStore;
import org.eclipse.rdf4j.sail.Sail;
import org.eclipse.rdf4j.sail.config.SailConfigException;
import org.eclipse.rdf4j.sail.config.SailFactory;
import org.eclipse.rdf4j.sail.config.SailImplConfig;

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
