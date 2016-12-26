package org.apache.hadoop.yarn.applications.tensorflow;

/**
 * Created by muzhongz on 16-12-19.
 */
public class TFParamServerAddress extends TFServerAddress{

    public TFParamServerAddress(ClusterSpec cluster, String address, int port, int taskIndex) {
        super(cluster, address, port, "ps", taskIndex);
    }
}
