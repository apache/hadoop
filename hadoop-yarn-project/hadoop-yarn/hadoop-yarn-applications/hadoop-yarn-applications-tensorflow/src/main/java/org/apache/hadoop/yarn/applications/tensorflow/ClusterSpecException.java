package org.apache.hadoop.yarn.applications.tensorflow;

/**
 * Created by muzhongz on 16-12-27.
 */
public class ClusterSpecException extends Exception {

    public ClusterSpecException() {
        super();
    }

    public ClusterSpecException(String message) {
        super(message);
    }

    public ClusterSpecException(String message, Throwable cause) {
        super(message, cause);
    }

    public ClusterSpecException(Throwable cause) {
        super(cause);
    }

    public ClusterSpecException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
