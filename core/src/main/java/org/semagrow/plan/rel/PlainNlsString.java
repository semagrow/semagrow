package org.semagrow.plan.rel;

import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.util.NlsString;

import java.util.Objects;

/**
 * Created by angel on 7/7/2017.
 */
public class PlainNlsString extends NlsString {

    private String language;

    /**
     * Creates a string in a specfied character set.
     *
     * @param value       String constant, must not be null
     * @param charsetName Name of the character set, may be null
     * @param collation   Collation, may be null
     * @throws IllegalCharsetNameException If the given charset name is illegal
     * @throws UnsupportedCharsetException If no support for the named charset
     *                                     is available in this instance of the Java virtual machine
     * @throws RuntimeException            If the given value cannot be represented in the
     *                                     given charset
     */
    public PlainNlsString(String value, String charsetName, SqlCollation collation) {
        super(value, charsetName, collation);
    }

    public PlainNlsString(String value, String charsetName, SqlCollation collation, String language) {
        super(value, charsetName, collation);
        this.language = language;
    }


    public void setLanguage(String language) {
        this.language = language;
    }

    @Override public boolean equals(Object o) {
        if (!(o instanceof PlainNlsString)) {
            return false;
        }
        PlainNlsString that = (PlainNlsString) o;
        return super.equals(o) && Objects.equals(language, that.language);
    }

}
