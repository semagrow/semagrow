package eu.semagrow.core.impl.alignment;

import eu.semagrow.core.transformation.QueryTransformation;
import org.eclipse.rdf4j.model.IRI;

/**
 * Created by angel on 12/2/14.
 */
public class URITransformer implements Transformer<IRI,IRI> {

    private QueryTransformation transformationService;

    private int id;

    private IRI sourceSchema, targetSchema;

    private double proximity = 1.0;

    public URITransformer(QueryTransformation transformationService, int id, IRI sourceSchema, IRI targetSchema) {
        this.transformationService = transformationService;
        this.id = id;
        this.sourceSchema = sourceSchema;
        this.targetSchema = targetSchema;
    }


    public int getId() { return id; }


    public IRI getSourceSchema() { return this.sourceSchema; }


    public IRI getTargetSchema() { return this.targetSchema; }


    public double getProximity() { return proximity; }

    public void setProximity(double proximity) { this.proximity = proximity; }


    public IRI transform(IRI source) {
        return transformationService.getURI(source, getId());
    }


    public IRI transformBack(IRI target) {
        return transformationService.getInvURI(target, getId());
    }

}
