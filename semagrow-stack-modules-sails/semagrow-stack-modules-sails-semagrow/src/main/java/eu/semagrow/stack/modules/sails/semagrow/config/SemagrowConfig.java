package eu.semagrow.stack.modules.sails.semagrow.config;

import eu.semagrow.stack.modules.sails.config.VOIDInferencerConfig;
import org.openrdf.sail.config.SailImplConfig;
import org.openrdf.sail.config.SailImplConfigBase;
import org.openrdf.sail.inferencer.fc.config.ForwardChainingRDFSInferencerConfig;
import org.openrdf.sail.memory.config.MemoryStoreConfig;

/**
 * Created by angel on 5/29/14.
 */
public class SemagrowConfig extends SailImplConfigBase {

    public SemagrowConfig() { super(SemagrowFactory.SAIL_TYPE); }

    public SailImplConfig getMetadataConfig() {
        return new VOIDInferencerConfig(
                new ForwardChainingRDFSInferencerConfig(
                        new MemoryStoreConfig()));
    }

    public String getMetadataFilename() {
        return "/tmp/metadata.ttl";
    }
}
