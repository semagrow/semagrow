package org.semagrow.plan.rel.type;

import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.vocabulary.XMLSchema;

import java.util.HashMap;
import java.util.Map;


public class RdfDataTypeFactoryImpl
        extends SqlTypeFactoryImpl
        implements RdfDataTypeFactory
{

    public RdfDataTypeFactoryImpl(RelDataTypeSystem typeSystem) {
        super(typeSystem);
    }

    @Override
    public RelDataType createRdfDataType(IRI dataType) {
        RdfDataType type = new RdfDataType(dataType);
        return canonize(type);
    }

    /**
     * Created by angel on 7/7/2017.
     */
    protected static class RdfDataType extends AbstractRdfType {

        private IRI datatype;

        static Map<IRI, SqlTypeName> dataTypetoSQL = new HashMap<>();

        static {
            dataTypetoSQL.put(XMLSchema.BOOLEAN, SqlTypeName.BOOLEAN);
            dataTypetoSQL.put(XMLSchema.INTEGER, SqlTypeName.DECIMAL);
            dataTypetoSQL.put(XMLSchema.INT, SqlTypeName.DECIMAL);
            dataTypetoSQL.put(XMLSchema.SHORT, SqlTypeName.DECIMAL);
            dataTypetoSQL.put(XMLSchema.LONG, SqlTypeName.DECIMAL);
            dataTypetoSQL.put(XMLSchema.DECIMAL, SqlTypeName.DECIMAL);
            dataTypetoSQL.put(XMLSchema.DOUBLE, SqlTypeName.DOUBLE);
            dataTypetoSQL.put(XMLSchema.DATE, SqlTypeName.DATE);
            dataTypetoSQL.put(XMLSchema.BASE64BINARY, SqlTypeName.BINARY);
            dataTypetoSQL.put(XMLSchema.STRING, SqlTypeName.CHAR);
        }

        public RdfDataType(IRI datatype) {
            this.datatype = Preconditions.checkNotNull(datatype);
            computeDigest();
        }

        @Override
        protected void generateTypeString(StringBuilder sb, boolean withDetail) {
            sb.append(datatype.toString());
        }

        @Override
        public SqlTypeName getSqlTypeName() {
            return dataTypetoSQL.get(datatype);
        }


    }
}
