package eu.semagrow.cassandra.mapping;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

/**
 * Created by antonis on 8/4/2016.
 */
public class RdfMapper {

    private static final ValueFactory vf = ValueFactoryImpl.getInstance();

    public static URI getUriFromTable(String base, String table) {
        String uriString = base + "/" + table;
        return vf.createURI(uriString);
    }

    public static URI getUriFromColumn(String base, String table, String column) {
        String uriString = base + "/" + table + "#" + column;
        return vf.createURI(uriString);
    }

    public static URI getXsdFromColumnDatatype(DataType dataType) {
        if (dataType.equals(DataType.ascii()) || dataType.equals(DataType.varchar()) || dataType.equals(DataType.text())) {
            return vf.createURI("http://www.w3.org/2001/XMLSchema#string");
        }
        if (dataType.equals(DataType.varint())) {
            return vf.createURI("http://www.w3.org/2001/XMLSchema#integer");
        }
        if (dataType.equals(DataType.decimal())) {
            return vf.createURI("http://www.w3.org/2001/XMLSchema#decimal");
        }
        if (dataType.equals(DataType.cint())) {
            return vf.createURI("http://www.w3.org/2001/XMLSchema#int");
        }
        if (dataType.equals(DataType.bigint()) || dataType.equals(DataType.counter()) ) {
            return vf.createURI("http://www.w3.org/2001/XMLSchema#long");
        }
        if (dataType.equals(DataType.cdouble())) {
            return vf.createURI("http://www.w3.org/2001/XMLSchema#double");
        }
        if (dataType.equals(DataType.cfloat())) {
            return vf.createURI("http://www.w3.org/2001/XMLSchema#float");
        }
        if (dataType.equals(DataType.cboolean())) {
            return vf.createURI("http://www.w3.org/2001/XMLSchema#boolean");
        }
        throw new RuntimeException();  /*blob, inet, timestamp, uuid, timeuuid, list, map, set*/
    }

    public static Value getLiteralFromCassandraResult(Row row, String columnname) {
        DataType dataType = row.getColumnDefinitions().getType(columnname);

        if (row.getObject(columnname) == null) {
            return vf.createLiteral("");
        }

        if (dataType.equals(DataType.ascii()) || dataType.equals(DataType.varchar()) || dataType.equals(DataType.text())) {
            String result = row.getString(columnname);
            if (result.startsWith("<") && result.endsWith(">")) {
                return vf.createURI(result.substring(1,result.length()-1));
            }
            else {
                return vf.createLiteral(result);
            }
        }
        if (dataType.equals(DataType.varint())) {
            return vf.createLiteral(row.getVarint(columnname).doubleValue());
        }
        if (dataType.equals(DataType.decimal())) {
            return vf.createLiteral(row.getDecimal(columnname).doubleValue());
        }
        if (dataType.equals(DataType.cint())) {
            return vf.createLiteral(row.getInt(columnname));
        }
        if (dataType.equals(DataType.bigint()) || dataType.equals(DataType.counter()) ) {
            return vf.createLiteral(row.getLong(columnname));
        }
        if (dataType.equals(DataType.cdouble())) {
            return vf.createLiteral(row.getDouble(columnname));
        }
        if (dataType.equals(DataType.cfloat())) {
            return vf.createLiteral(row.getFloat(columnname));
        }
        if (dataType.equals(DataType.cboolean())) {
            return vf.createLiteral(row.getBool(columnname));
        }
        throw new RuntimeException();  /*blob, inet, timestamp, uuid, timeuuid, list, map, set, isNULL */
    }


}
