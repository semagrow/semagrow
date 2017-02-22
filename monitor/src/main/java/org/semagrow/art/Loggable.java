package org.semagrow.art;

import java.lang.annotation.*;

/**
 * Created by angel on 2/10/2015.
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE })
@SuppressWarnings({
        "PMD.VariableNamingConventions", "PMD.RedundantFieldInitializer"
})
public @interface Loggable {

    /**
     * TRACE level of logging.
     */
    int TRACE = 0;

    /**
     * DEBUG level of logging.
     */
    int DEBUG = 1;

    /**
     * INFO level of logging.
     */
    int INFO = 2;

    /**
     * WARN level of logging.
     */
    int WARN = 3;

    /**
     * ERROR level of logging.
     */
    int ERROR = 4;

    /**
     * Level of logging.
     */
    int value() default Loggable.INFO;
}
