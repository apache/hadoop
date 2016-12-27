package org.apache.hadoop.yarn.applications.tensorflow;

import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;

/**
 * Created by muzhongz on 16-12-27.
 */
public interface TFApplicationRpc {
    public String getClusterSpec() throws IOException, YarnException;
}
