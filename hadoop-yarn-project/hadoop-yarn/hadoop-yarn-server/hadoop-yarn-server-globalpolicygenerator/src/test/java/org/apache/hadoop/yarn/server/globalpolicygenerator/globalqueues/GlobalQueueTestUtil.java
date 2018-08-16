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
package org.apache.hadoop.yarn.server.globalpolicygenerator.globalqueues;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.federation.store.records.SubClusterId;

/**
 * This class provides support methods for all global queue tests.
 */
public final class GlobalQueueTestUtil {

  private GlobalQueueTestUtil() {
  }

  public static List<FederationQueue> generateFedCluster() throws IOException {
    int numSubclusters = 20;

    List<FederationQueue> toMerge = new ArrayList<>();
    String queueJson = loadFile("globalqueues/tree-queue.json");
    for (int i = 0; i < numSubclusters; i++) {
      FederationQueue temp = parseQueue(queueJson);
      toMerge.add(temp);
    }
    return toMerge;
  }

  public static String loadFile(String filename) throws IOException {
    ClassLoader cL = Thread.currentThread().getContextClassLoader();
    if (cL == null) {
      cL = Configuration.class.getClassLoader();
    }
    URL submitURI = cL.getResource(filename);

    return FileUtils.readFileToString(new File(submitURI.getFile()),
        Charset.defaultCharset());
  }

  public static String toJSONString(Resource resInf) {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append("\"memory\"");
    builder.append(" : ");
    builder.append(resInf.getMemorySize());
    builder.append(", ");
    builder.append("\"vCores\"");
    builder.append(" : ");
    builder.append(resInf.getVirtualCores());
    builder.append("}");
    return builder.toString();
  }

  public static FederationQueue parseQueue(String queueJson)
      throws IOException {
    JsonFactory jsonF = new JsonFactory();
    ObjectMapper mapper = new ObjectMapper();
    Map jsonMap = mapper.readValue(jsonF.createParser(queueJson), Map.class);
    FederationQueue fq = parseFedQueue(jsonMap);
    fq.propagateCapacities();
    return fq;
  }

  private static FederationQueue parseFedQueue(Map jsonMap) {
    FederationQueue fq = new FederationQueue(new Configuration());
    fq.setQueueName(jsonMap.get("queueName").toString());
    fq.setSubClusterId(SubClusterId.newInstance(
        ((Map)(jsonMap.get("scope"))).get("id").toString()));
    if (jsonMap.get("guarCap") != null) {
      fq.setGuarCap(parseResource((Map)jsonMap.get("guarCap")));
    }
    if (jsonMap.get("maxCap") != null) {
      fq.setMaxCap(parseResource((Map)jsonMap.get("maxCap")));
    }
    if (jsonMap.get("usedCap") != null) {
      fq.setUsedCap(parseResource((Map)jsonMap.get("usedCap")));
    }
    if (jsonMap.get("totCap") != null) {
      fq.setTotCap(parseResource((Map)jsonMap.get("totCap")));
    }
    if (jsonMap.get("demandCap") != null) {
      fq.setDemandCap(parseResource((Map)jsonMap.get("demandCap")));
    }
    if (jsonMap.get("children") != null) {
      List children = (List) jsonMap.get("children");
      Map<String, FederationQueue> childrenFedQueue = new HashMap<>();
      for (Object o : children) {
        FederationQueue child = parseFedQueue((Map)(o));
        childrenFedQueue.put(child.getQueueName(), child);
      }
      fq.setChildren(childrenFedQueue);
    }
    return fq;
  }

  private static Resource parseResource(Map resMap) {
    Resource res = Resource.newInstance(0, 0);
    if (resMap.get("memory") != null) {
      res.setMemorySize(Integer.parseInt(resMap.get("memory").toString()));
    }
    if (resMap.get("vCores") != null) {
      res.setVirtualCores(Integer.parseInt(resMap.get("vCores").toString()));
    }
    return res;
  }
}
