/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.applications.tensorflow;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.util.ThreadUtil;
import org.apache.hadoop.yarn.applications.tensorflow.api.TensorflowCluster;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.GetClusterSpecRequest;
import org.apache.hadoop.yarn.applications.tensorflow.api.protocolrecords.GetClusterSpecResponse;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.apache.hadoop.yarn.ipc.YarnRPC;

import java.io.IOException;
import java.net.InetSocketAddress;


public class TFApplicationRpcServer implements TensorflowCluster, Runnable {
    private int rpcPort = -1;
    private String rpcAddress = null;

    public int getRpcPort() {
        return rpcPort;
    }

    public void setRpcPort(int rpcPort) {
        this.rpcPort = rpcPort;
    }

    public String getRpcAddress() {
        return rpcAddress;
    }

    public void setRpcAddress(String rpcAddress) {
        this.rpcAddress = rpcAddress;
    }

    private static final RecordFactory recordFactory =
            RecordFactoryProvider.getRecordFactory(null);

    private TFApplicationRpc appRpc = null;
    private Server server;

    public TFApplicationRpcServer(String hostname, TFApplicationRpc rpc) {
        this.setRpcAddress(hostname);
        this.setRpcPort(10000 + ((int)(Math.random() * (5000)) + 1));
        this.appRpc = rpc;
    }

    @Override
    public GetClusterSpecResponse getClusterSpec(GetClusterSpecRequest request) throws YarnException, IOException {
        GetClusterSpecResponse response = recordFactory.newRecordInstance(GetClusterSpecResponse.class);
        response.setClusterSpec(this.appRpc.getClusterSpec());
        return response;
    }

    public void startRpcServiceThread() {
        Thread thread = new Thread(this);
        thread.start();
    }

    @Override
    public void run() {
        Configuration conf = new Configuration();
        YarnRPC rpc = YarnRPC.create(conf);
        InetSocketAddress address = new InetSocketAddress(rpcAddress, rpcPort);
        this.server = rpc.getServer(
                TensorflowCluster.class, this, address, conf, null,
                conf.getInt(YarnConfiguration.RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT,
                        YarnConfiguration.DEFAULT_RM_RESOURCE_TRACKER_CLIENT_THREAD_COUNT));

        this.server.start();
    }
}
