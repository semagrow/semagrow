package org.semagrow.plan.rel.semagrow;

import org.apache.calcite.rel.type.RelDataType;

import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class TypeUtil {

    public static Function<Object[],Object[]> getAccessor(final List<Integer> fields)
    {
        return new Function<Object[], Object[]>() {
            @Override
            public Object[] apply(Object[] objects) {
                Object[] result = new Object[fields.size()];
                int i = 0;
                for (Integer field : fields) {
                    result[i] = objects[field];
                    i++;
                }
                return result;
            }
        };
    }


    public static BiFunction<Object[],Object[],Object[]> getJoinSelector(final RelDataType resultType,
                                                                         final RelDataType leftType,
                                                                         final RelDataType rightType) {
        return new BiFunction<Object[], Object[], Object[]>() {
            @Override
            public Object[] apply(Object[] left, Object[] right) {
                Object[] result = new Object[resultType.getFieldCount()];

                int j = 0;

                if (left == null) {
                    for (int i = 0; i < leftType.getFieldCount(); i++) {
                        result[j] = null;
                        j++;
                    }
                } else {
                    for (int i = 0; i < leftType.getFieldCount(); i++) {
                        result[j] = left[i];
                        j++;
                    }
                }

                if (right == null) {
                    for (int i = 0; i < rightType.getFieldCount(); i++) {
                        result[j] = null;
                        j++;
                    }
                } else {
                    for (int i = 0; i < rightType.getFieldCount(); i++) {
                        result[j] = right[i];
                        j++;
                    }
                }
                return result;
            }
        };
    }


}
