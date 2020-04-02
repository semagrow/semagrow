package org.semagrow.plan.rel.sparql;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;
import org.semagrow.plan.rel.type.RDFTypes;

/**
 * Created by angel on 16/7/2017.
 */
public class SparqlStdOperatorTable extends ReflectiveSqlOperatorTable {

    public static final SparqlFunction IS_NUMERIC;//sparql10
    public static final SparqlFunction IS_LITERAL;//sparql10
    public static final SparqlFunction IS_RESOURCE;//sparql10
    public static final SparqlFunction IS_URI;//sparql10
    public static final SparqlFunction LANG_MATCHES;//sparql10
    public static final SparqlFunction LANG;//sparql10
    public static final SparqlFunction LABEL;
    public static final SparqlFunction DATATYPE;

    public static final SparqlFunction SAMETERM = null; //sparql10
    public static final SparqlFunction STR = null;//sparql10
    public static final SparqlFunction BNODE = null;//sparql11
    public static final SparqlFunction STRDT = null;
    public static final SparqlFunction STRLANG = null;
    public static final SparqlFunction UUID = null;
    public static final SparqlFunction STRUUID = null;
    public static final SparqlFunction REGEX = null;
    public static final SparqlFunction STRLEN = null;
    public static final SparqlFunction STRSTARTS = null;
    public static final SparqlFunction STRENDS = null;
    public static final SparqlFunction STRBEFORE = null;
    public static final SparqlFunction STRAFTER = null;
    public static final SparqlFunction ENCODE_FOR_URI = null;
    public static final SqlFunction SUBSTR  = sparql11(SqlStdOperatorTable.SUBSTRING);
    public static final SqlFunction UCASE   = sparql11(SqlStdOperatorTable.UPPER);
    public static final SqlFunction LCASE   = sparql11(SqlStdOperatorTable.LOWER);
    public static final SqlOperator CONCAT  = sparql11(SqlStdOperatorTable.CONCAT);
    public static final SqlFunction REPLACE = sparql11(SqlStdOperatorTable.REPLACE);
    public static final SqlFunction CONTAINS = null;

    public static final SqlOperator AND = sparql10(SqlStdOperatorTable.AND);
    public static final SqlOperator OR  = sparql10(SqlStdOperatorTable.OR);
    public static final SqlOperator NOT = sparql10(SqlStdOperatorTable.NOT);

    public static final SqlOperator EQUALS = sparql10(SqlStdOperatorTable.EQUALS);
    public static final SqlOperator GREATER_THAN = sparql10(SqlStdOperatorTable.GREATER_THAN);
    public static final SqlOperator LESS_THAN = sparql10(SqlStdOperatorTable.LESS_THAN);
    public static final SqlOperator LESS_THAN_OR_EQUAL = sparql10(SqlStdOperatorTable.LESS_THAN_OR_EQUAL);
    public static final SqlOperator GREATER_THAN_OR_EQUAL = sparql10(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL);

    public static final SqlFunction ABS = sparql11(SqlStdOperatorTable.ABS);
    public static final SqlFunction CEIL = sparql11(SqlStdOperatorTable.CEIL);
    public static final SqlFunction FLOOR = sparql11(SqlStdOperatorTable.FLOOR);
    public static final SqlFunction RAND = sparql11(SqlStdOperatorTable.RAND);
    public static final SqlFunction ROUND = sparql11(SqlStdOperatorTable.ROUND);

    public static final SqlFunction NOW = sparql11(SqlStdOperatorTable.CURRENT_TIME);
    public static final SqlFunction YEAR = sparql11(SqlStdOperatorTable.YEAR);
    public static final SqlFunction MONTH = sparql11(SqlStdOperatorTable.MONTH);
    public static final SqlFunction DAY = sparql11(SqlStdOperatorTable.DAYOFYEAR);
    public static final SqlFunction HOURS = sparql11(SqlStdOperatorTable.HOUR);
    public static final SqlFunction MINUTES = sparql11(SqlStdOperatorTable.MINUTE);
    public static final SqlFunction SECONDS = sparql11(SqlStdOperatorTable.SECOND);
    public static final SqlFunction TIMEZONE = null;
    public static final SqlFunction TZ = null;

    public static final SparqlFunction MD5 = null;
    public static final SparqlFunction SHA1 = null;
    public static final SparqlFunction SHA256 = null;
    public static final SparqlFunction SHA384 = null;
    public static final SparqlFunction SHA512 = null;

    public static final SqlOperator PLUS = sparql10(SqlStdOperatorTable.PLUS);
    public static final SqlOperator MINUS = sparql10(SqlStdOperatorTable.MINUS);
    public static final SqlOperator MULTIPLY = sparql10(SqlStdOperatorTable.MULTIPLY);
    public static final SqlOperator DIVIDE = sparql10(SqlStdOperatorTable.DIVIDE);

    public static final SqlOperator LIKE = sparql11(SqlStdOperatorTable.LIKE);
    public static final SqlFunction COALESCE = sparql11(SqlStdOperatorTable.COALESCE);
    public static final SqlOperator BOUND = sparql10(SqlStdOperatorTable.IS_NOT_NULL);

    public static final SqlOperator IF = sparql11(SqlStdOperatorTable.CASE);

    public static final SqlAggFunction COUNT = sparql11(SqlStdOperatorTable.COUNT);
    public static final SqlAggFunction SUM   = sparql11(SqlStdOperatorTable.SUM);
    public static final SqlAggFunction AVG   = sparql11(SqlStdOperatorTable.AVG);
    public static final SqlAggFunction MAX   = sparql11(SqlStdOperatorTable.MAX);
    public static final SqlAggFunction MIN   = sparql11(SqlStdOperatorTable.MIN);
    public static final SqlAggFunction SAMPLE;
    public static final SqlAggFunction GROUPCONCAT;


    static {
        IS_NUMERIC  = new SparqlFunction("ISNUMERIC", ReturnTypes.BOOLEAN_NOT_NULL, (SqlOperandTypeInference)null, OperandTypes.NUMERIC_INTEGER, SqlFunctionCategory.STRING, SparqlVersion.SPARQL10);
        IS_LITERAL  = new SparqlFunction("ISLITERAL", ReturnTypes.BOOLEAN_NOT_NULL, (SqlOperandTypeInference)null, OperandTypes.NUMERIC_INTEGER, SqlFunctionCategory.STRING, SparqlVersion.SPARQL10);
        IS_RESOURCE = new SparqlFunction("ISRESOURCE", ReturnTypes.BOOLEAN_NOT_NULL, (SqlOperandTypeInference)null, OperandTypes.NUMERIC_INTEGER, SqlFunctionCategory.STRING, SparqlVersion.SPARQL10);
        IS_URI      = new SparqlFunction("ISURI", ReturnTypes.BOOLEAN_NOT_NULL, (SqlOperandTypeInference)null, OperandTypes.NUMERIC_INTEGER, SqlFunctionCategory.STRING, SparqlVersion.SPARQL10);
        LANG_MATCHES= new SparqlFunction("LANGMATCHES", ReturnTypes.BOOLEAN_NOT_NULL, (SqlOperandTypeInference)null, OperandTypes.NUMERIC_INTEGER, SqlFunctionCategory.STRING, SparqlVersion.SPARQL10);

        LANG     = new SparqlFunction("LANG", ReturnTypes.explicit(RDFTypes.PLAIN_LITERAL), (SqlOperandTypeInference)null, OperandTypes.NUMERIC_INTEGER, SqlFunctionCategory.STRING, SparqlVersion.SPARQL10);
        LABEL    = new SparqlFunction("LABEL", ReturnTypes.explicit(RDFTypes.PLAIN_LITERAL), (SqlOperandTypeInference)null, OperandTypes.NUMERIC_INTEGER, SqlFunctionCategory.STRING, SparqlVersion.SPARQL10);
        DATATYPE = new SparqlFunction("DATATYPE", ReturnTypes.explicit(RDFTypes.PLAIN_LITERAL), (SqlOperandTypeInference)null, OperandTypes.NUMERIC_INTEGER, SqlFunctionCategory.STRING, SparqlVersion.SPARQL10);

        SAMPLE      = new SparqlSampleAggFunction();
        GROUPCONCAT = new SparqlGroupConcatAggFunction();
    }

    public static SqlOperator makeSparqlFunction(String uri, int params) {
        return new SparqlIRIFunction(
                uri,
                ReturnTypes.ARG0,
                null,
                OperandTypes.VARIADIC,
                SparqlVersion.SPARQL10);
    }

    static <T> T sparql10(T op) { return SparqlFeature.sparql10(op); }

    static <T> T sparql11(T op) { return SparqlFeature.sparql11(op); }

    static class SparqlSampleAggFunction extends SqlAggFunction {

        protected SparqlSampleAggFunction() {
            super("SAMPLE",
                null,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.ARG0,
                null,
                OperandTypes.ANY,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                false,
                false);
        }
    }

    static class SparqlGroupConcatAggFunction extends SqlAggFunction {

        protected SparqlGroupConcatAggFunction() {
            super("GROUPCONCAT",
                null,
                SqlKind.OTHER_FUNCTION,
                ReturnTypes.ARG0,
                null,
                OperandTypes.ANY,
                SqlFunctionCategory.USER_DEFINED_FUNCTION,
                false,
                false);
        }
    }

    static class SparqlIRIFunction extends SparqlFunction {

        public SparqlIRIFunction(String name,
                                 SqlReturnTypeInference returnTypeInference,
                                 SqlOperandTypeInference operandTypeInference,
                                 SqlOperandTypeChecker operandTypeChecker,
                                 SparqlVersion requiredVersion) {
            super(name,
                    returnTypeInference,
                    operandTypeInference,
                    operandTypeChecker,
                    requiredVersion);
        }

        @Override public boolean isStandard() { return false; }
    }


}
