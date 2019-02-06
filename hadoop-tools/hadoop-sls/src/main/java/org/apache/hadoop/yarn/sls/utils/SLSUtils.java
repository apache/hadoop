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
package org.apache.hadoop.yarn.sls.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.apache.hadoop.yarn.api.records.NodeLabel;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceInformation;
import org.apache.hadoop.yarn.client.util.YarnClientUtils;
import org.apache.hadoop.yarn.sls.SLSRunner.NodeDetails;
import org.apache.hadoop.yarn.sls.conf.SLSConfiguration;
import org.apache.hadoop.yarn.util.resource.ResourceUtils;
import org.apache.hadoop.yarn.util.resource.Resources;

@Private
@Unstable
public class SLSUtils {
  public final static String DEFAULT_JOB_TYPE = "mapreduce";

  private static final String LABEL_FORMAT_ERR_MSG =
      "Input format for adding node-labels is not correct, it should be "
          + "labelName1[(exclusive=true/false)],labelName2[] ..";

  // hostname includes the network path and the host name. for example
  // "/default-rack/hostFoo" or "/coreSwitchA/TORSwitchB/hostBar".
  // the function returns two Strings, the first element is the network
  // location without "/", the second element is the host name. for example,
  // {"default-rack", "hostFoo"} or "coreSwitchA/TORSwitchB", "hostBar"
  public static String[] getRackHostName(String hostname) {
    NodeBase node = new NodeBase(hostname);
    return new String[] {node.getNetworkLocation().substring(1),
        node.getName()};
  }

  /**
   * parse the rumen trace file, return each host name
   */
  public static Set<NodeDetails> parseNodesFromRumenTrace(
      String jobTrace) throws IOException {
    Set<NodeDetails> nodeSet = new HashSet<>();

    File fin = new File(jobTrace);
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "file:///");
    JobTraceReader reader = new JobTraceReader(
            new Path(fin.getAbsolutePath()), conf);
    try {
      LoggedJob job = null;
      while ((job = reader.getNext()) != null) {
        for(LoggedTask mapTask : job.getMapTasks()) {
          // select the last attempt
          if (mapTask.getAttempts().size() == 0) {
            continue;
          }
          LoggedTaskAttempt taskAttempt = mapTask.getAttempts()
                  .get(mapTask.getAttempts().size() - 1);
          nodeSet.add(new NodeDetails(
              taskAttempt.getHostName().getValue()));
        }
        for(LoggedTask reduceTask : job.getReduceTasks()) {
          if (reduceTask.getAttempts().size() == 0) {
            continue;
          }
          LoggedTaskAttempt taskAttempt = reduceTask.getAttempts()
                  .get(reduceTask.getAttempts().size() - 1);
          nodeSet.add(new NodeDetails(
              taskAttempt.getHostName().getValue()));
        }
      }
    } finally {
      reader.close();
    }

    return nodeSet;
  }

  /**
   * parse the sls trace file, return each host name
   */
  public static Set<NodeDetails> parseNodesFromSLSTrace(
      String jobTrace) throws IOException {
    Set<NodeDetails> nodeSet = new HashSet<>();
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    Reader input =
        new InputStreamReader(new FileInputStream(jobTrace), "UTF-8");
    try {
      Iterator<Map> i = mapper.readValues(jsonF.createParser(input), Map.class);
      while (i.hasNext()) {
        addNodes(nodeSet, i.next());
      }
    } finally {
      input.close();
    }
    return nodeSet;
  }

  private static void addNodes(Set<NodeDetails> nodeSet,
      Map jsonEntry) {
    if (jsonEntry.containsKey(SLSConfiguration.NUM_NODES)) {
      int numNodes = Integer.parseInt(
          jsonEntry.get(SLSConfiguration.NUM_NODES).toString());
      int numRacks = 1;
      if (jsonEntry.containsKey(SLSConfiguration.NUM_RACKS)) {
        numRacks = Integer.parseInt(
            jsonEntry.get(SLSConfiguration.NUM_RACKS).toString());
      }
      nodeSet.addAll(generateNodes(numNodes, numRacks));
    }

    if (jsonEntry.containsKey(SLSConfiguration.JOB_TASKS)) {
      List tasks = (List) jsonEntry.get(SLSConfiguration.JOB_TASKS);
      for (Object o : tasks) {
        Map jsonTask = (Map) o;
        String hostname = (String) jsonTask.get(SLSConfiguration.TASK_HOST);
        if (hostname != null) {
          nodeSet.add(new NodeDetails(hostname));
        }
      }
    }
  }

  /**
   * parse the input node file, return each host name
   * sample input: label1(exclusive=true),label2(exclusive=false),label3
   */
  public static Set<NodeDetails> parseNodesFromNodeFile(
      String nodeFile, Resource nmDefaultResource) throws IOException {
    Set<NodeDetails> nodeSet = new HashSet<>();
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    Reader input =
        new InputStreamReader(new FileInputStream(nodeFile), "UTF-8");
    try {
      Iterator<Map> i = mapper.readValues(jsonF.createParser(input), Map.class);
      while (i.hasNext()) {
        Map jsonE = i.next();
        String rack = "/" + jsonE.get("rack");
        List tasks = (List) jsonE.get("nodes");
        for (Object o : tasks) {
          Map jsonNode = (Map) o;
          NodeDetails nodeDetails = new NodeDetails(
              rack + "/" + jsonNode.get("node"));
          Resource nodeResource = Resources.clone(nmDefaultResource);
          ResourceInformation[] infors = ResourceUtils.getResourceTypesArray();
          for (ResourceInformation info : infors) {
            if (jsonNode.get(info.getName()) != null) {
              nodeResource.setResourceValue(info.getName(),
                  Integer.parseInt(jsonNode.get(info.getName()).toString()));
            }
          }
          nodeDetails.setNodeResource(nodeResource);
          if (jsonNode.get("labels") != null) {
            Set<NodeLabel> nodeLabels =  new HashSet<>(
                YarnClientUtils.buildNodeLabelsFromStr(
                    jsonNode.get("labels").toString()));
            nodeDetails.setLabels(nodeLabels);
          }
          nodeSet.add(nodeDetails);
        }
      }
    } finally {
      input.close();
    }
    return nodeSet;
  }

  public static Set<NodeDetails> generateNodes(int numNodes,
      int numRacks){
    Set<NodeDetails> nodeSet = new HashSet<>();
    if (numRacks < 1) {
      numRacks = 1;
    }

    if (numRacks > numNodes) {
      numRacks = numNodes;
    }

    for (int i = 0; i < numNodes; i++) {
      nodeSet.add(new NodeDetails(
          "/rack" + i % numRacks + "/node" + i));
    }
    return nodeSet;
  }
}
