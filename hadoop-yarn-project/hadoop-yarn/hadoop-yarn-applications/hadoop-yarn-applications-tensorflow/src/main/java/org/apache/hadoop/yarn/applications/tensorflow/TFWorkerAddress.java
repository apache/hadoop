package org.apache.hadoop.yarn.applications.tensorflow;

/**
 * Created by muzhongz on 16-12-19.
 */
public class TFWorkerAddress extends TFServerAddress {

    public TFWorkerAddress(ClusterSpec cluster, String address, int port, int taskIndex) {
        super(cluster, address, port, "worker", taskIndex);
    }

}
