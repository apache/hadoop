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

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice.tensorflow;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.apache.hadoop.yarn.submarine.common.Envs;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceUtils;

public class TensorFlowConfigEnvGenerator {
  private static final ObjectMapper OBJECT_MAPPER = createObjectMapper();

  private static ObjectMapper createObjectMapper() {
    ObjectMapper mapper = new ObjectMapper();
    mapper.setAnnotationIntrospector(
        new JaxbAnnotationIntrospector(TypeFactory.defaultInstance()));
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, false);
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper;
  }

  public static String getTFConfigEnv(String componentName, int nWorkers,
      int nPs, String serviceName, String userName, String domain) {
    String commonEndpointSuffix = YarnServiceUtils
        .getDNSNameCommonSuffix(serviceName, userName, domain, 8000);

    TFConfigEnv tfConfigEnv =
        new TFConfigEnv(nWorkers, nPs, componentName, commonEndpointSuffix);
    return tfConfigEnv.toJson();
  }

  private static class TFConfigEnv {
    private final int nWorkers;
    private final int nPS;
    private final String componentName;
    private final String endpointSuffix;

    TFConfigEnv(int nWorkers, int nPS, String componentName,
        String endpointSuffix) {
      this.nWorkers = nWorkers;
      this.nPS = nPS;
      this.componentName = componentName;
      this.endpointSuffix = endpointSuffix;
    }

    String toJson() {
      ObjectNode rootNode = OBJECT_MAPPER.createObjectNode();

      ObjectNode cluster = rootNode.putObject("cluster");
      createComponentArray(cluster, "master", 1);
      createComponentArray(cluster, "worker", nWorkers - 1);
      createComponentArray(cluster, "ps", nPS);

      ObjectNode task = rootNode.putObject("task");
      task.put("type", componentName);
      task.put("index", "$" + Envs.TASK_INDEX_ENV);
      task.put("environment", "cloud");
      try {
        return OBJECT_MAPPER.writeValueAsString(rootNode);
      } catch (JsonProcessingException e) {
        throw new RuntimeException("Failed to serialize TF config env JSON!",
            e);
      }
    }

    private void createComponentArray(ObjectNode cluster, String name,
        int count) {
      ArrayNode array = cluster.putArray(name);
      for (int i = 0; i < count; i++) {
        String componentValue = String.format("%s-%d%s", name, i,
            endpointSuffix);
        array.add(componentValue);
      }
    }
  }
}
