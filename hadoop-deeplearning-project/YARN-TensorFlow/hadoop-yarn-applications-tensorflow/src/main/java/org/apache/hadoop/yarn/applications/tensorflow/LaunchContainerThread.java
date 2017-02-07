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

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.*;

import java.io.IOException;
import java.util.*;

public class LaunchContainerThread implements Runnable {

    private static final Log LOG = LogFactory.getLog(LaunchContainerThread.class);

    private Container container;
    private String tfServerJar;
    private long containerMemory = 10;

    // Container retry options
    private ContainerRetryPolicy containerRetryPolicy =
            ContainerRetryPolicy.NEVER_RETRY;
    private Set<Integer> containerRetryErrorCodes = null;
    private int containerMaxRetries = 0;
    private int containrRetryInterval = 0;

    private ApplicationMaster appMaster;

    private TFServerAddress serverAddress = null;

    public String getTfServerJar() {
        return tfServerJar;
    }

    public void setTfServerJar(String tfServerJar) {
        this.tfServerJar = tfServerJar;
    }

    public long getContainerMemory() {
        return containerMemory;
    }

    public void setContainerMemory(long containerMemory) {
        this.containerMemory = containerMemory;
    }

    public ContainerRetryPolicy getContainerRetryPolicy() {
        return containerRetryPolicy;
    }

    public void setContainerRetryPolicy(ContainerRetryPolicy containerRetryPolicy) {
        this.containerRetryPolicy = containerRetryPolicy;
    }

    public Set<Integer> getContainerRetryErrorCodes() {
        return containerRetryErrorCodes;
    }

    public void setContainerRetryErrorCodes(Set<Integer> containerRetryErrorCodes) {
        this.containerRetryErrorCodes = containerRetryErrorCodes;
    }

    public int getContainerMaxRetries() {
        return containerMaxRetries;
    }

    public void setContainerMaxRetries(int containerMaxRetries) {
        this.containerMaxRetries = containerMaxRetries;
    }

    public int getContainrRetryInterval() {
        return containrRetryInterval;
    }

    public void setContainrRetryInterval(int containrRetryInterval) {
        this.containrRetryInterval = containrRetryInterval;
    }

    private LaunchContainerThread(Container container, ApplicationMaster appMaster) {
        this.container = container;
        this.appMaster = appMaster;
    }

    public LaunchContainerThread(Container container, ApplicationMaster appMaster, TFServerAddress serverAddress) {
        this(container, appMaster);
        this.serverAddress = serverAddress;
        if (this.serverAddress == null) {
            LOG.info("server address is null");
        }
    }

    @Override
    /**
     * Connects to CM, sets up container launch context
     * for shell command and eventually dispatches the container
     * start request to the CM.
     */
    public void run() {
        LOG.info("Setting up container launch container for containerid="
                + container.getId());

        FileSystem fs = null;
        try {
            fs = FileSystem.get(appMaster.getConfiguration());
        } catch (IOException e) {
            e.printStackTrace();
        }

        TFContainer tfContainer = new TFContainer(appMaster);

        Map<String, String> env = tfContainer.setJavaEnv(appMaster.getConfiguration(), null);

        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

        ApplicationId appId = appMaster.getAppAttempId().getApplicationId();

        try {
            tfContainer.addToLocalResources(fs, tfServerJar, TFContainer.SERVER_JAR_PATH, localResources);
        } catch (IOException e) {
            e.printStackTrace();
        }


        LOG.info("clusterspec: " + this.serverAddress.getClusterSpec().toString());
        //this.serverAddress.getClusterSpec().testClusterString();
        ClusterSpec cs = this.serverAddress.getClusterSpec();

        StringBuilder command = null;
        try {
            command = tfContainer.makeCommands(containerMemory,
                    cs.getBase64EncodedJsonString(),
                    this.serverAddress.getJobName(),
                    this.serverAddress.getTaskIndex());
        } catch (JsonProcessingException e) {
            LOG.info("cluster spec cannot convert into base64 json string!");
            e.printStackTrace();
        } catch (ClusterSpecException e) {
            e.printStackTrace();
        }

        List<String> commands = new ArrayList<String>();
         commands.add(command.toString());
        if (serverAddress != null) {
            LOG.info(serverAddress.getJobName() + " : " + serverAddress.getAddress() + ":" + serverAddress.getPort());
        }

        ContainerRetryContext containerRetryContext =
                ContainerRetryContext.newInstance(
                        containerRetryPolicy, containerRetryErrorCodes,
                        containerMaxRetries, containrRetryInterval);
        for (String cmd : commands) {
            LOG.info("Container " + container.getId() + " command: " + cmd.toString());
        }
        ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                localResources, env, commands, null, appMaster.getAllTokens().duplicate(),
                null, containerRetryContext);
        appMaster.addContainer(container);
        appMaster.getNMClientAsync().startContainerAsync(container, ctx);
    }

}