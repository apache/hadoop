/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.sls;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.resourcemanager.ResourceManager;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.sls.SLSRunner.TraceType;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.sls.nodemanager.NMSimulator;
import org.apache.hadoop.yarn.sls.scheduler.TaskRunner;
import org.apache.hadoop.yarn.sls.synthetic.SynthTraceJobProducer;
import org.apache.hadoop.yarn.sls.utils.SLSUtils;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class NMRunner {
  private static final Logger LOG = LoggerFactory.getLogger(NMRunner.class);
  
  // other simulation information
  private int numNMs, numRacks;

  // NM simulator
  private Map<NodeId, NMSimulator> nmMap;
  private Resource nodeManagerResource;
  private String nodeFile;
  private TaskRunner taskRunner;
  private Configuration conf;
  private ResourceManager rm;
  private String tableMapping;
  private int threadPoolSize;
  private TraceType inputType;
  private String[] inputTraces;
  private SynthTraceJobProducer stjp;

  public NMRunner(TaskRunner taskRunner, Configuration conf, ResourceManager rm, String tableMapping, int threadPoolSize) {
    this.taskRunner = taskRunner;
    this.conf = conf;
    this.rm = rm;
    this.tableMapping = tableMapping;
    this.threadPoolSize = threadPoolSize;
    this.nmMap = new ConcurrentHashMap<>();
    this.nodeManagerResource = getNodeManagerResourceFromConf();
  }

  public void startNM() throws YarnException, IOException,
      InterruptedException {
    // nm configuration
    int heartbeatInterval = conf.getInt(
        SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS,
        SLSConfiguration.NM_HEARTBEAT_INTERVAL_MS_DEFAULT);
    float resourceUtilizationRatio = conf.getFloat(
        SLSConfiguration.NM_RESOURCE_UTILIZATION_RATIO,
        SLSConfiguration.NM_RESOURCE_UTILIZATION_RATIO_DEFAULT);
    // nm information (fetch from topology file, or from sls/rumen json file)
    Set<SLSRunner.NodeDetails> nodeSet = null;
    if (nodeFile.isEmpty()) {
      for (String inputTrace : inputTraces) {
        switch (inputType) {
          case SLS:
            nodeSet = SLSUtils.parseNodesFromSLSTrace(inputTrace);
            break;
          case RUMEN:
            nodeSet = SLSUtils.parseNodesFromRumenTrace(inputTrace);
            break;
          case SYNTH:
            stjp = new SynthTraceJobProducer(conf, new Path(inputTraces[0]));
            nodeSet = SLSUtils.generateNodes(stjp.getNumNodes(),
                stjp.getNumNodes()/stjp.getNodesPerRack());
            break;
          default:
            throw new YarnException("Input configuration not recognized, "
                + "trace type should be SLS, RUMEN, or SYNTH");
        }
      }
    } else {
      nodeSet = SLSUtils.parseNodesFromNodeFile(nodeFile,
          nodeManagerResource);
    }

    if (nodeSet == null || nodeSet.isEmpty()) {
      throw new YarnException("No node! Please configure nodes.");
    }

    SLSUtils.generateNodeTableMapping(nodeSet, tableMapping);

    // create NM simulators
    Random random = new Random();
    Set<String> rackSet = ConcurrentHashMap.newKeySet();
    int threadPoolSize = Math.max(this.threadPoolSize,
        SLSConfiguration.RUNNER_POOL_SIZE_DEFAULT);
    ExecutorService executorService = Executors.
        newFixedThreadPool(threadPoolSize);
    for (SLSRunner.NodeDetails nodeDetails : nodeSet) {
      executorService.submit(new Runnable() {
        @Override public void run() {
          try {
            // we randomize the heartbeat start time from zero to 1 interval
            NMSimulator nm = new NMSimulator();
            Resource nmResource = nodeManagerResource;
            String hostName = nodeDetails.getHostname();
            if (nodeDetails.getNodeResource() != null) {
              nmResource = nodeDetails.getNodeResource();
            }
            Set<NodeLabel> nodeLabels = nodeDetails.getLabels();
            nm.init(hostName, nmResource,
                random.nextInt(heartbeatInterval),
                heartbeatInterval, rm, resourceUtilizationRatio, nodeLabels);
            nmMap.put(nm.getNode().getNodeID(), nm);
            taskRunner.schedule(nm);
            rackSet.add(nm.getNode().getRackName());
          } catch (IOException | YarnException e) {
            LOG.error("Got an error while adding node", e);
          }
        }
      });
    }
    executorService.shutdown();
    executorService.awaitTermination(10, TimeUnit.MINUTES);
    numRacks = rackSet.size();
    numNMs = nmMap.size();
  }

  void waitForNodesRunning() throws InterruptedException {
    long startTimeMS = System.currentTimeMillis();
    while (true) {
      int numRunningNodes = 0;
      for (RMNode node : rm.getRMContext().getRMNodes().values()) {
        if (node.getState() == NodeState.RUNNING) {
          numRunningNodes++;
        }
      }
      if (numRunningNodes == numNMs) {
        break;
      }
      LOG.info("SLSRunner is waiting for all nodes RUNNING."
          + " {} of {} NMs initialized.", numRunningNodes, numNMs);
      Thread.sleep(1000);
    }
    LOG.info("SLSRunner takes {} ms to launch all nodes.",
        System.currentTimeMillis() - startTimeMS);
  }

  private Resource getNodeManagerResourceFromConf() {
    Resource resource = Resources.createResource(0);
    ResourceInformation[] infors = ResourceUtils.getResourceTypesArray();
    for (ResourceInformation info : infors) {
      long value;
      if (info.getName().equals(ResourceInformation.MEMORY_URI)) {
        value = conf.getInt(SLSConfiguration.NM_MEMORY_MB,
            SLSConfiguration.NM_MEMORY_MB_DEFAULT);
      } else if (info.getName().equals(ResourceInformation.VCORES_URI)) {
        value = conf.getInt(SLSConfiguration.NM_VCORES,
            SLSConfiguration.NM_VCORES_DEFAULT);
      } else {
        value = conf.getLong(SLSConfiguration.NM_PREFIX +
            info.getName(), SLSConfiguration.NM_RESOURCE_DEFAULT);
      }

      resource.setResourceValue(info.getName(), value);
    }

    return resource;
  }

  public void setNodeFile(String nodeFile) {
    this.nodeFile = nodeFile;
  }


  public void setInputType(TraceType inputType) {
    this.inputType = inputType;
  }

  public void setInputTraces(String[] inputTraces) {
    this.inputTraces = inputTraces.clone();
  }

  public int getNumNMs() {
    return numNMs;
  }

  public int getNumRacks() {
    return numRacks;
  }

  public Resource getNodeManagerResource() {
    return nodeManagerResource;
  }

  public Map<NodeId, NMSimulator> getNmMap() {
    return nmMap;
  }

  public SynthTraceJobProducer getStjp() {
    return stjp;
  }

  public void setTableMapping(String tableMapping) {
    this.tableMapping = tableMapping;
  }

  public void setRm(ResourceManager rm) {
    this.rm = rm;
  }
}
