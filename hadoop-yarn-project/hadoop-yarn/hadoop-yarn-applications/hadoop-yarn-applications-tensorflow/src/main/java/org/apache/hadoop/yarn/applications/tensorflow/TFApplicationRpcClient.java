package org.apache.hadoop.yarn.applications.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.retry.RetryPolicy;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.yarn.applications.tensorflow.api.*;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.GetClusterSpecRequest;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.GetClusterSpecResponse;
import org.apache.hadoop.yarn.client.RMProxy;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Created by muzhongz on 16-12-27.
 */
public class TFApplicationRpcClient implements TFApplicationRpc {
    private String serverAddress;
    private int serverPort;
    private RecordFactory recordFactory = RecordFactoryProvider.getRecordFactory(null);
    private TensorflowCluster tensorflow;

    public TFApplicationRpcClient(String serverAddress, int serverPort) {
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
    }

    public String getClusterSpec() throws IOException, YarnException {
        GetClusterSpecResponse response =
                this.tensorflow.getClusterSpec(recordFactory.newRecordInstance(GetClusterSpecRequest.class));
        return response.getClusterSpec();
    }

    public TFApplicationRpc getRpc() {
        InetSocketAddress address = new InetSocketAddress(serverAddress, serverPort);
        Configuration conf = new Configuration();
        RetryPolicy retryPolicy = RMProxy.createRetryPolicy(conf, false);
        try {
            TensorflowCluster proxy = RMProxy.createRMProxy(conf, TensorflowCluster.class, address);
            this.tensorflow = (TensorflowCluster) RetryProxy.create(
                    TensorflowCluster.class, proxy, retryPolicy);
            return this;
        } catch (IOException e) {
            return null;
        }
    }
}
