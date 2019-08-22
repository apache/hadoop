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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.ServiceApiConstants;
import org.apache.hadoop.yarn.service.api.records.Component;
import org.apache.hadoop.yarn.submarine.common.Envs;
import org.apache.hadoop.yarn.submarine.common.api.Role;
import org.apache.hadoop.yarn.submarine.runtimes.yarnservice.YarnServiceUtils;

import java.util.Map;

/**
 * This class has common helper methods for TensorFlow.
 */
public final class TensorFlowCommons {
  private TensorFlowCommons() {
    throw new UnsupportedOperationException("This class should not be " +
        "instantiated!");
  }

  public static void addCommonEnvironments(Component component,
      Role role) {
    Map<String, String> envs = component.getConfiguration().getEnv();
    envs.put(Envs.TASK_INDEX_ENV, ServiceApiConstants.COMPONENT_ID);
    envs.put(Envs.TASK_TYPE_ENV, role.getName());
  }

  public static String getUserName() {
    return System.getProperty("user.name");
  }

  public static String getDNSDomain(Configuration yarnConfig) {
    return yarnConfig.get("hadoop.registry.dns.domain-name");
  }

  public static String getScriptFileName(Role role) {
    return "run-" + role.getName() + ".sh";
  }

  public static String getTFConfigEnv(String componentName, int nWorkers,
      int nPs, String serviceName, String userName, String domain) {
    String commonEndpointSuffix = YarnServiceUtils
        .getDNSNameCommonSuffix(serviceName, userName, domain, 8000);

    String json = "{\\\"cluster\\\":{";

    String master = getComponentArrayJson("master", 1, commonEndpointSuffix)
        + ",";
    String worker = getComponentArrayJson("worker", nWorkers - 1,
        commonEndpointSuffix) + ",";
    String ps = getComponentArrayJson("ps", nPs, commonEndpointSuffix) + "},";

    StringBuilder sb = new StringBuilder();
    sb.append("\\\"task\\\":{");
    sb.append(" \\\"type\\\":\\\"");
    sb.append(componentName);
    sb.append("\\\",");
    sb.append(" \\\"index\\\":");
    sb.append('$');
    sb.append(Envs.TASK_INDEX_ENV + "},");
    String task = sb.toString();
    String environment = "\\\"environment\\\":\\\"cloud\\\"}";

    sb = new StringBuilder();
    sb.append(json);
    sb.append(master);
    sb.append(worker);
    sb.append(ps);
    sb.append(task);
    sb.append(environment);
    return sb.toString();
  }

  private static String getComponentArrayJson(String componentName, int count,
      String endpointSuffix) {
    String component = "\\\"" + componentName + "\\\":";
    StringBuilder array = new StringBuilder();
    array.append("[");
    for (int i = 0; i < count; i++) {
      array.append("\\\"");
      array.append(componentName);
      array.append("-");
      array.append(i);
      array.append(endpointSuffix);
      array.append("\\\"");
      if (i != count - 1) {
        array.append(",");
      }
    }
    array.append("]");
    return component + array.toString();
  }
}
