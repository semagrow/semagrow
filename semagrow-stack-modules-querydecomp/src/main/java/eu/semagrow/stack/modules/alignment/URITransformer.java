package eu.semagrow.stack.modules.alignment;

import eu.semagrow.stack.modules.api.transformation.QueryTransformation;
import org.openrdf.model.URI;

/**
 * Created by angel on 12/2/14.
 */
public class URITransformer implements Transformer<URI,URI> {

    private QueryTransformation transformationService;

    private int id;

    private URI sourceSchema, targetSchema;

    private double proximity = 1.0;

    public URITransformer(QueryTransformation transformationService, int id, URI sourceSchema, URI targetSchema) {
        this.transformationService = transformationService;
        this.id = id;
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
    }


    public int getId() { return id; }


    public URI getSourceSchema() { return this.sourceSchema; }


    public URI getTargetSchema() { return this.targetSchema; }


    public double getProximity() { return proximity; }

    public void setProximity(double proximity) { this.proximity = proximity; }


    public URI transform(URI source) {
        return transformationService.getURI(source, getId());
    }


    public URI transformBack(URI target) {
        return transformationService.getInvURI(target, getId());
    }

}
