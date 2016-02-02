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

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NodeBase;
import org.apache.hadoop.tools.rumen.JobTraceReader;
import org.apache.hadoop.tools.rumen.LoggedJob;
import org.apache.hadoop.tools.rumen.LoggedTask;
import org.apache.hadoop.tools.rumen.LoggedTaskAttempt;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.map.ObjectMapper;

@Private
@Unstable
public class SLSUtils {

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
  public static Set<String> parseNodesFromRumenTrace(String jobTrace)
          throws IOException {
    Set<String> nodeSet = new HashSet<String>();

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
          nodeSet.add(taskAttempt.getHostName().getValue());
        }
        for(LoggedTask reduceTask : job.getReduceTasks()) {
          if (reduceTask.getAttempts().size() == 0) {
            continue;
          }
          LoggedTaskAttempt taskAttempt = reduceTask.getAttempts()
                  .get(reduceTask.getAttempts().size() - 1);
          nodeSet.add(taskAttempt.getHostName().getValue());
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
  public static Set<String> parseNodesFromSLSTrace(String jobTrace)
          throws IOException {
    Set<String> nodeSet = new HashSet<String>();
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    Reader input =
        new InputStreamReader(new FileInputStream(jobTrace), "UTF-8");
    try {
      Iterator<Map> i = mapper.readValues(
              jsonF.createJsonParser(input), Map.class);
      while (i.hasNext()) {
        Map jsonE = i.next();
        List tasks = (List) jsonE.get("job.tasks");
        for (Object o : tasks) {
          Map jsonTask = (Map) o;
          String hostname = jsonTask.get("container.host").toString();
          nodeSet.add(hostname);
        }
      }
    } finally {
      input.close();
    }
    return nodeSet;
  }

  /**
   * parse the input node file, return each host name
   */
  public static Set<String> parseNodesFromNodeFile(String nodeFile)
          throws IOException {
    Set<String> nodeSet = new HashSet<String>();
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    Reader input =
        new InputStreamReader(new FileInputStream(nodeFile), "UTF-8");
    try {
      Iterator<Map> i = mapper.readValues(
              jsonF.createJsonParser(input), Map.class);
      while (i.hasNext()) {
        Map jsonE = i.next();
        String rack = "/" + jsonE.get("rack");
        List tasks = (List) jsonE.get("nodes");
        for (Object o : tasks) {
          Map jsonNode = (Map) o;
          nodeSet.add(rack + "/" + jsonNode.get("node"));
        }
      }
    } finally {
      input.close();
    }
    return nodeSet;
  }
}
