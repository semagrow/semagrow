package org.semagrow.config;

import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

/**
 * @author Angelos Charalambidis
 */
public class SourceSelectorImplConfigBase implements SourceSelectorImplConfig {

    private String type;

    public SourceSelectorImplConfigBase() { }

    public SourceSelectorImplConfigBase(String type) { this(); setType(type); }

    public String getType() { return type; }

    public void setType(String type) { this.type = type; }

    public void validate() throws SourceSelectorConfigException {

    }

    public Resource export(Model graph) {
        ValueFactory vf = SimpleValueFactory.getInstance();
        BNode implNode = vf.createBNode();
        graph.add(implNode, SemagrowSchema.SOURCESELECTOR, vf.createLiteral(getType()));
        return implNode;
    }

    public void parse(Model graph, Resource resource) throws SourceSelectorConfigException {

    }
}
