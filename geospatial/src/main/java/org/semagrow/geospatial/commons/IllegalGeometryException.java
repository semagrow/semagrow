package org.semagrow.geospatial.commons;

public class IllegalGeometryException extends Exception {

    public IllegalGeometryException(String message) {
            super(message);
        }

    public IllegalGeometryException(String message, Throwable cause) {
            super(message,cause);
        }

    public IllegalGeometryException(Throwable cause) {
            super(cause);
        }
}
