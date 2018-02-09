package org.semagrow.plan.rel.sparql;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

/**
 * A non-standard SPARQL FunctionCall using functor name a IRI.
 * */
public class SparqlFunction extends SqlFunction implements SparqlFeature {

    private SparqlVersion requiredVersion;

    public SparqlFunction(String name,
                          SqlReturnTypeInference returnTypeInference,
                          SqlOperandTypeInference operandTypeInference,
                          SqlOperandTypeChecker operandTypeChecker,
                          SparqlVersion requiredVersion)
    {
        this(name,
            returnTypeInference,
            operandTypeInference,
            operandTypeChecker,
            SqlFunctionCategory.USER_DEFINED_FUNCTION,
            requiredVersion);
    }

    public SparqlFunction(String name,
                          SqlReturnTypeInference returnTypeInference,
                          SqlOperandTypeInference operandTypeInference,
                          SqlOperandTypeChecker operandTypeChecker,
                          SqlFunctionCategory sqlFunctionCategory,
                          SparqlVersion requiredVersion)
    {
        super(name,
                SqlKind.OTHER_FUNCTION,
                returnTypeInference,
                operandTypeInference,
                operandTypeChecker,
                sqlFunctionCategory);

        this.requiredVersion = requiredVersion;
    }


    public boolean isStandard() { return true; }

    public SparqlVersion getRequiredSparqlVersion() { return this.requiredVersion; }

}
