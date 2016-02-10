package eu.semagrow.hibiscus.config;

import eu.semagrow.core.config.SemagrowSchema;
import eu.semagrow.core.config.SourceSelectorConfigException;
import eu.semagrow.core.config.SourceSelectorImplConfigBase;
import org.openrdf.model.Graph;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.util.GraphUtil;
import org.openrdf.model.util.GraphUtilException;

/**
 * Created by angel on 16/6/2015.
 */
public class QuetsalSourceSelectorImplConfig extends SourceSelectorImplConfigBase
{
    private String summariesFile;
    private String metadataFile;
    private String mode;
    private double commonPredicateThreshold;

    public QuetsalSourceSelectorImplConfig(String type) { super(type); }

    public String getSummariesFile() {
        return summariesFile;
    }

    public void setSummariesFile(String summariesFile) {
        this.summariesFile = summariesFile;
    }

    public String getMetadataFile() {
        return metadataFile;
    }

    public void setMetadataFile(String metadataFile) {
        this.metadataFile = metadataFile;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public double getCommonPredicateThreshold() {
        return commonPredicateThreshold;
    }

    public void setCommonPredicateThreshold(double commonPredicateThreshold) {
        this.commonPredicateThreshold = commonPredicateThreshold;
    }


    @Override
    public Resource export(Graph graph) {
        Resource node = super.export(graph);
        ValueFactory vf = graph.getValueFactory();

        if (mode != null)
            graph.add(node, QuetsalSchema.MODE, vf.createLiteral(mode));


        graph.add(node, QuetsalSchema.COMMONPREDTHREASHOLD, vf.createLiteral(commonPredicateThreshold));

        if (summariesFile != null)
            graph.add(node, QuetsalSchema.SUMMARIES, vf.createLiteral(summariesFile));

        return node;
    }

    @Override
    public void parse(Graph graph, Resource resource)
            throws SourceSelectorConfigException
    {
        try {
            Literal summariesLit = GraphUtil.getOptionalObjectLiteral(graph, resource, QuetsalSchema.SUMMARIES);
            if (summariesLit != null) {
                summariesFile = summariesLit.getLabel();
            }

            Literal metadataLit = GraphUtil.getOptionalObjectLiteral(graph, resource, SemagrowSchema.METADATAINIT);
            if (metadataLit != null) {
                metadataFile = metadataLit.getLabel();
            }

            Literal modeLit = GraphUtil.getOptionalObjectLiteral(graph, resource, QuetsalSchema.MODE);

            if (modeLit != null) {
                mode = modeLit.getLabel();
            }

            Literal commonPredLit = GraphUtil.getOptionalObjectLiteral(graph, resource, QuetsalSchema.COMMONPREDTHREASHOLD);

            if (commonPredLit != null) {
                commonPredicateThreshold = commonPredLit.doubleValue();
            }

        } catch (GraphUtilException e) {
            throw new SourceSelectorConfigException(e);
        }

    }
}
