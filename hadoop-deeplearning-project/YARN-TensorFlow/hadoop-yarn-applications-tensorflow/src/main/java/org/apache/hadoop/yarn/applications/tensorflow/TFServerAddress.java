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

public class TFServerAddress {
    private String address;
    private int port;
    private String jobName; /* worker or ps */
    private int taskIndex;
    private ClusterSpec clusterSpec;

    protected TFServerAddress(ClusterSpec cluster, String address, int port, String jobName, int taskIndex) {
        this.setClusterSpec(cluster);
        this.setAddress(address);
        this.setPort(port);
        this.setJobName(jobName);
        this.setTaskIndex(taskIndex);
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public int getTaskIndex() {
        return taskIndex;
    }

    public void setTaskIndex(int taskIndex) {
        this.taskIndex = taskIndex;
    }

    public ClusterSpec getClusterSpec() {
        return clusterSpec;
    }

    public void setClusterSpec(ClusterSpec clusterSpec) {
        this.clusterSpec = clusterSpec;
    }
}
