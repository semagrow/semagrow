/**
 * 
 */
package eu.semagrow.core.impl.alignment;

import org.eclipse.rdf4j.model.IRI;

import eu.semagrow.core.transformation.EquivalentURI;


/* (non-Javadoc)
 * @see eu.semagrow.core.transformation.EquivalentURI()
 */
public class EquivalentURIImpl implements EquivalentURI {

    private IRI source, target;
    private IRI sourceSchema, targetSchema;
    private int proximity;
    private int transformationID;

    public EquivalentURIImpl(IRI source, IRI target, IRI sourceSchema, IRI targetSchema, int proximity, int transformationID) {
        this.source = source;
        this.target = target;
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.proximity = proximity;
        this.transformationID = transformationID;
    }

    public IRI getSourceURI() {
        return source;
    }

    public void setSourceURI(IRI source) {
        this.source = source;
    }

    public IRI getTargetURI() {
        return target;
    }

    public void setTargetURI(IRI target) {
        this.target = target;
    }

    public IRI getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(IRI sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    public IRI getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(IRI targetSchema) {
        this.targetSchema = targetSchema;
    }

    public int getProximity() {
        return proximity;
    }

    public void setProximity(int proximity) {
        this.proximity = proximity;
    }

    public int getTransformationID() {
        return transformationID;
    }

    public void setTransformationID(int transformationID) {
        this.transformationID = transformationID;
    }

}
