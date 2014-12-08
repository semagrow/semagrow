/**
 * 
 */
package eu.semagrow.stack.modules.alignment;

import org.openrdf.model.URI;

import eu.semagrow.stack.modules.api.transformation.EquivalentURI;


/* (non-Javadoc)
 * @see eu.semagrow.stack.modules.api.transformation.EquivalentURI()
 */
public class EquivalentURIImpl implements EquivalentURI {

    private URI source, target;
    private URI sourceSchema, targetSchema;
    private int proximity;
    private int transformationID;

    public EquivalentURIImpl(URI source, URI target, URI sourceSchema, URI targetSchema, int proximity, int transformationID) {
        this.source = source;
        this.target = target;
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
        this.proximity = proximity;
        this.transformationID = transformationID;
    }

    public URI getSourceURI() {
        return source;
    }

    public void setSourceURI(URI source) {
        this.source = source;
    }

    public URI getTargetURI() {
        return target;
    }

    public void setTargetURI(URI target) {
        this.target = target;
    }

    public URI getSourceSchema() {
        return sourceSchema;
    }

    public void setSourceSchema(URI sourceSchema) {
        this.sourceSchema = sourceSchema;
    }

    public URI getTargetSchema() {
        return targetSchema;
    }

    public void setTargetSchema(URI targetSchema) {
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
