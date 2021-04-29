package org.semagrow.sail.config;

import org.semagrow.config.*;
import org.semagrow.selector.SourceSelectorRegistry;
import org.semagrow.util.FileUtils;
import org.eclipse.rdf4j.model.*;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.model.util.Models;
import org.eclipse.rdf4j.repository.config.RepositoryImplConfig;
import org.eclipse.rdf4j.repository.sail.config.SailRepositoryConfig;
import org.eclipse.rdf4j.sail.config.*;
import org.eclipse.rdf4j.sail.inferencer.fc.config.ForwardChainingRDFSInferencerConfig;
import org.eclipse.rdf4j.sail.memory.config.MemoryStoreConfig;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/**
 * Created by angel on 5/29/14.
 */
public class SemagrowSailConfig extends AbstractSailImplConfig {

    private String metadataRepoId = "semagrow_metadata";

    private List<String> filenames = new LinkedList<String>();
    private int executorBatchSize = 10;

    private String queryTransformationUser;
    private String queryTransformationPassword;
    private String queryTransformationDBString;

    private SourceSelectorImplConfig sourceSelectorConfig = null;

    public SemagrowSailConfig() { super(SemagrowSailFactory.SAIL_TYPE); }

    public SourceSelectorImplConfig getSourceSelectorConfig() {

        if (sourceSelectorConfig != null)
            return sourceSelectorConfig;
        else
            return new RepositorySourceSelectorConfig();
    }

    public String getMetadataRepoId() { return metadataRepoId; }

    public void setMetadataRepoId(String metadataId) { metadataRepoId = metadataId; }

    public RepositoryImplConfig getMetadataConfig() {

        SailImplConfig sailConfig = new org.semagrow.sail.sevod.config.SEVODInferencerConfig(
                new ForwardChainingRDFSInferencerConfig(
                        new MemoryStoreConfig()));


        return new SailRepositoryConfig(sailConfig);
    }

    public List<String> getInitialFiles() {
        if (filenames.isEmpty()) {
            List<String> autoFiles = new LinkedList<String>();
            try {
                File f =  FileUtils.getFile("metadata.ttl");
                autoFiles.add(f.getAbsolutePath());
                return autoFiles;
            } catch (IOException e) {
                return filenames;
            }
        } else {
            return filenames;
        }
    }

    public void addInitialFiles( java.util.Collection<String> files )
    {
    	filenames.addAll( files );
    }

    public void setExecutorBatchSize(int b) {
        executorBatchSize = b;
    }

    public int getExecutorBatchSize() {
        return executorBatchSize;
    }

    @Override
    public Resource export(Model graph) {
        Resource implNode = super.export(graph);
        ValueFactory vf = SimpleValueFactory.getInstance();
        for (String file : filenames) {
            graph.add(implNode, SemagrowSchema.METADATAINIT, vf.createLiteral(file));
        }

        String queryTransfDB = getQueryTransformationDB();
        if (queryTransfDB != null) {
            graph.add(implNode, SemagrowSchema.QUERYTRANSFORMDB, vf.createLiteral(queryTransfDB));
            graph.add(implNode, SemagrowSchema.QUERYTRANSFORMUSER, vf.createLiteral(getQueryTransformationUser()));
            graph.add(implNode, SemagrowSchema.QUERYTRANSFORMPASSWORD, vf.createLiteral(getQueryTransformationPassword()));
        }
        return implNode;
    }

    @Override
    public void parse(Model graph, Resource node) throws SailConfigException {

        for (Value o : graph.filter(node, SemagrowSchema.METADATAINIT, null).objects())
        {
            filenames.add(o.stringValue());
        }

        for (Value o : graph.filter(node, SemagrowSchema.EXECUTORBATCHSIZE, null).objects()) {
            executorBatchSize = Integer.parseInt(o.stringValue());
        }

        /*
        try {
            Literal dbLit = GraphUtil.getOptionalObjectLiteral(graph, node, SemagrowSchema.QUERYTRANSFORMDB);
            Literal dbUser = GraphUtil.getOptionalObjectLiteral(graph, node, SemagrowSchema.QUERYTRANSFORMUSER);
            Literal dbPass = GraphUtil.getOptionalObjectLiteral(graph, node, SemagrowSchema.QUERYTRANSFORMPASSWORD);

            setQueryTransformationDB(dbLit.stringValue());
            setQueryTransformationAuth(dbUser.stringValue(), dbPass.stringValue());

        } catch (GraphUtilException e) {
            e.printStackTrace();
        }
        */

        Optional<Literal> sourceSelectorImplNode = Models.objectLiteral(graph.filter(node, SemagrowSchema.SOURCESELECTOR,null));

        if (sourceSelectorImplNode.isPresent()) {

            Optional<SourceSelectorFactory> factory = SourceSelectorRegistry.getInstance().get(sourceSelectorImplNode.get().getLabel());

            if (factory.isPresent()) {
                throw new SailConfigException("Unsupported source selector type: " + sourceSelectorImplNode.get().getLabel());
            }

            sourceSelectorConfig = factory.get().getConfig();
            try {
                sourceSelectorConfig.parse(graph, node);
            } catch (SourceSelectorConfigException e) {
                throw new SailConfigException(e);
            }
        }

        super.parse(graph, node);
    }

    public String getQueryTransformationDB() {
        return this.queryTransformationDBString;
    }

    public void setQueryTransformationDB(String dbString) { this.queryTransformationDBString = dbString; }

    public String getQueryTransformationUser() { return this.queryTransformationUser; }

    public String getQueryTransformationPassword() { return this.queryTransformationPassword; }

    public void setQueryTransformationAuth(String username, String password) {
        this.queryTransformationUser = username;
        this.queryTransformationPassword = password;
    }

    public boolean hasSelectorConfig() { return (sourceSelectorConfig != null); }
}
