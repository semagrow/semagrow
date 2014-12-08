package eu.semagrow.stack.modules.sails.semagrow.config;

import org.openrdf.repository.config.RepositoryImplConfig;
import org.openrdf.repository.sail.config.ProxyRepositoryConfig;
import eu.semagrow.stack.modules.sails.config.SEVODInferencerConfig;
import org.openrdf.repository.sail.config.SailRepositoryConfig;
import org.openrdf.sail.config.SailImplConfig;
import org.openrdf.sail.inferencer.fc.config.ForwardChainingRDFSInferencerConfig;
import org.openrdf.sail.memory.config.MemoryStoreConfig;

import java.util.List;

/**
 * Created by angel on 11/1/14.
 */
public class RepositorySourceSelectorConfig extends SourceSelectorImplConfigBase {

    public RepositoryImplConfig getMetadataConfig() {
        /*
        SailImplConfig sailConfig = new SEVODInferencerConfig(
                                        new ForwardChainingRDFSInferencerConfig(
                                            new MemoryStoreConfig()));


        return new SailRepositoryConfig(sailConfig);
        */
        return new ProxyRepositoryConfig("semagrow_metadata");
    }

}
