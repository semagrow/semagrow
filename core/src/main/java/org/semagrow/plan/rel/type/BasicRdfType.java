package org.semagrow.plan.rel.type;

import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;

/**
 * Created by angel on 7/7/2017.
 */
public class BasicRdfType extends AbstractRdfType {


    private final String name;

    public BasicRdfType(String name) { this.name = name; computeDigest(); }

    @Override
    protected void generateTypeString(StringBuilder sb, boolean withDetail) {
        sb.append(name);
    }

    @Override
    public SqlTypeName getSqlTypeName() { return SqlTypeName.CHAR; }

    @Override
    public SqlCollation getCollation() { return SqlCollation.COERCIBLE; }

    @Override
    public Charset getCharset() { return Charset.forName("ISO-8859-1"); }
}
