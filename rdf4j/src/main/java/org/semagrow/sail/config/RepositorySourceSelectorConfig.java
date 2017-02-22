package org.semagrow.sail.config;

import org.semagrow.config.SourceSelectorImplConfigBase;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.sail.config.ProxyRepositoryConfig;

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
