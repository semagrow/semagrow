package org.semagrow.plan.rel;


import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;
import org.semagrow.plan.rel.type.RDFTypes;
import org.semagrow.plan.rel.type.RdfDataTypeFactory;
import org.semagrow.plan.rel.type.RdfDataTypeFactoryImpl;

import java.math.BigDecimal;


/**
 * Created by angel on 6/7/2017.
 */
public class RdfRexBuilder extends RexBuilder {

    private RdfDataTypeFactory rdfTypeFactory;
    /**
     * Creates a RexBuilder.
     *
     * @param typeFactory Type factory
     */
    public RdfRexBuilder(RdfDataTypeFactory typeFactory) {

        super(typeFactory);
        rdfTypeFactory = typeFactory;
    }

    public RexNode makeRDFLiteral(Literal l) {
        //new PlainNlsString()
        RelDataType type = getRelDataType(l);
        SqlTypeName sqlTypeName = type.getSqlTypeName();
        Comparable lit = unwrapLit(l);

        return makeLiteral(unwrapLit(l), type, sqlTypeName);
    }

    public RexNode makeRDFResource(Resource r) {
        RelDataType type = getRelDataType(r);
        SqlTypeName sqlTypeName = type.getSqlTypeName();

        return makeLiteral(new NlsString(r.stringValue(), null, null), type, sqlTypeName);
    }

    public RelDataType getRelDataType(Literal l) {
        if (l.getDatatype() == null)
            return RDFTypes.PLAIN_LITERAL;
        else {
            // recycle if already have been create the same type.
            return rdfTypeFactory.createRdfDataType(l.getDatatype());
        }
    }

    public RelDataType getRelDataType(Resource r) {
        if (r instanceof IRI)
            return RDFTypes.IRI_TYPE;
        else if (r instanceof BNode)
            return RDFTypes.BLANK_NODE;
        else
            return null;
    }


    public Comparable unwrapLit(Literal l) {

        IRI dt = l.getDatatype();

        if (dt == null) {
            PlainNlsString s = new PlainNlsString(l.stringValue(), null, null);
            l.getLanguage().ifPresent(lang -> s.setLanguage(lang));
            return s;
        } else {

            /** <tt>http://www.w3.org/2001/XMLSchema#integer</tt> */
            if (dt.equals(XMLSchema.INTEGER))
                return new BigDecimal(l.integerValue());
            else if (dt.equals(XMLSchema.INT))
                return BigDecimal.valueOf(l.intValue());
            else if (dt.equals(XMLSchema.DECIMAL) || dt.equals(XMLSchema.DOUBLE))
                return l.decimalValue();
            else if (dt.equals(XMLSchema.DATE) || dt.equals(XMLSchema.DATETIME))
                //return l.calendarValue().;
                return null;
            else if (dt.equals(XMLSchema.BOOLEAN))
                return l.booleanValue();
            else if (dt.equals(XMLSchema.BASE64BINARY))
                return ByteString.ofBase64(l.stringValue());
            else if (dt.equals(XMLSchema.STRING))
                return new NlsString(l.stringValue(),  "ISO-8859-1", SqlCollation.COERCIBLE);
            else
                return null;
        }
    }

}
