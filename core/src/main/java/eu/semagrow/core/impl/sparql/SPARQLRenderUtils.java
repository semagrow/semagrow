package eu.semagrow.core.impl.sparql;

import eu.semagrow.commons.PlainLiteral;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.util.Literals;
import org.eclipse.rdf4j.queryrender.RenderUtils;

/**
 * Created by angel on 8/6/2016.
 */
public class SPARQLRenderUtils {


    public static String toSPARQL(Value theValue) {
        StringBuilder aBuffer = toSPARQL(theValue, new StringBuilder());
        return aBuffer.toString();
    }

    public static StringBuilder toSPARQL(Value value, StringBuilder builder) {
        if(value instanceof IRI) {
            IRI aLit = (IRI)value;
            builder.append("<").append(aLit.toString()).append(">");
        } else if(value instanceof BNode) {
            builder.append("_:").append(((BNode)value).getID());
        } else if(value instanceof Literal) {
            Literal aLit1 = (Literal)value;
            builder.append("\"\"\"").append(RenderUtils.escape(aLit1.getLabel())).append("\"\"\"");
            if(Literals.isLanguageLiteral(aLit1)) {
                builder.append("@").append(aLit1.getLanguage());
            } else if (value instanceof PlainLiteral) {
                // do not print type
            } else {
                builder.append("^^<").append(aLit1.getDatatype().toString()).append(">");
            }
        }

        return builder;
    }


}
