package eu.semagrow.stack.modules.sails.semagrow.config;

import eu.semagrow.modules.fileutils.FileUtils;
import eu.semagrow.stack.modules.sails.config.SEVODInferencerConfig;
import org.openrdf.model.*;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.util.GraphUtilException;
import org.openrdf.repository.config.RepositoryImplConfig;
import org.openrdf.repository.sail.config.SailRepositoryConfig;
import org.openrdf.sail.config.SailConfigException;
import org.openrdf.sail.config.SailImplConfig;
import org.openrdf.sail.config.SailImplConfigBase;
import org.openrdf.sail.inferencer.fc.config.ForwardChainingRDFSInferencerConfig;
import org.openrdf.sail.memory.config.MemoryStoreConfig;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Created by angel on 5/29/14.
 */
public class SemagrowSailConfig extends SailImplConfigBase {

    private String metadataRepoId = "semagrow_metadata";

    private List<String> filenames = new LinkedList<String>();

    private String queryTransformationUser;
    private String queryTransformationPassword;
    private String queryTransformationDBString;

    public SemagrowSailConfig() { super(SemagrowSailFactory.SAIL_TYPE); }

    public SourceSelectorImplConfig getSourceSelectorConfig() {
        return new RepositorySourceSelectorConfig();
    }

    public String getMetadataRepoId() { return metadataRepoId; }

    public void setMetadataRepoId(String metadataId) { metadataRepoId = metadataId; }

    public RepositoryImplConfig getMetadataConfig() {

        SailImplConfig sailConfig = new SEVODInferencerConfig(
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

    public void setInitialFiles(List<String> files) { filenames = new LinkedList<String>(files); }

    @Override
    public Resource export(Graph graph) {
        Resource implNode = super.export(graph);
        for (String file : filenames) {
            graph.add(implNode, SemagrowSchema.METADATAINIT, graph.getValueFactory().createLiteral(file));
        }

        String queryTransfDB = getQueryTransformationDB();
        if (queryTransfDB != null) {
            graph.add(implNode, SemagrowSchema.QUERYTRANSFORMDB, graph.getValueFactory().createLiteral(queryTransfDB));
            graph.add(implNode, SemagrowSchema.QUERYTRANSFORMUSER, graph.getValueFactory().createLiteral(getQueryTransformationUser()));
            graph.add(implNode, SemagrowSchema.QUERYTRANSFORMPASSWORD, graph.getValueFactory().createLiteral(getQueryTransformationPassword()));
        }
        return implNode;
    }

    @Override
    public void parse(Graph graph, Resource node) throws SailConfigException {

        for (Value o : GraphUtil.getObjects(graph, node, SemagrowSchema.METADATAINIT))
        {
            filenames.add(o.stringValue());
        }

        try {
            Literal dbLit = GraphUtil.getOptionalObjectLiteral(graph, node, SemagrowSchema.QUERYTRANSFORMDB);
            Literal dbUser = GraphUtil.getOptionalObjectLiteral(graph, node, SemagrowSchema.QUERYTRANSFORMUSER);
            Literal dbPass = GraphUtil.getOptionalObjectLiteral(graph, node, SemagrowSchema.QUERYTRANSFORMPASSWORD);

            setQueryTransformationDB(dbLit.stringValue());
            setQueryTransformationAuth(dbUser.stringValue(), dbPass.stringValue());
        } catch (GraphUtilException e) {
            e.printStackTrace();
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
}
