/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.yarn.submarine.runtimes.yarnservice;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.service.api.records.Service;
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.submarine.common.Envs;
import org.apache.hadoop.yarn.submarine.common.conf.SubmarineLogs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class YarnServiceUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(YarnServiceUtils.class);

  // This will be true only in UT.
  private static ServiceClient stubServiceClient = null;

  public static ServiceClient createServiceClient(
      Configuration yarnConfiguration) {
    if (stubServiceClient != null) {
      return stubServiceClient;
    }

    ServiceClient serviceClient = new ServiceClient();
    serviceClient.init(yarnConfiguration);
    serviceClient.start();
    return serviceClient;
  }

  @VisibleForTesting
  public static void setStubServiceClient(ServiceClient stubServiceClient) {
    YarnServiceUtils.stubServiceClient = stubServiceClient;
  }

  public static String getDNSName(String serviceName,
      String componentInstanceName, String userName, String domain, int port) {
    return componentInstanceName + getDNSNameCommonSuffix(serviceName, userName,
        domain, port);
  }

  private static String getDNSNameCommonSuffix(String serviceName,
      String userName, String domain, int port) {
    String commonEndpointSuffix =
        "." + serviceName + "." + userName + "." + domain + ":" + port;
    return commonEndpointSuffix;
  }

  public static String getTFConfigEnv(String curCommponentName, int nWorkers,
      int nPs, String serviceName, String userName, String domain) {
    String commonEndpointSuffix = getDNSNameCommonSuffix(serviceName, userName,
        domain, 8000);

    String json = "{\\\"cluster\\\":{";

    String master = getComponentArrayJson("master", 1, commonEndpointSuffix)
        + ",";
    String worker = getComponentArrayJson("worker", nWorkers - 1,
        commonEndpointSuffix) + ",";
    String ps = getComponentArrayJson("ps", nPs, commonEndpointSuffix) + "},";

    StringBuilder sb = new StringBuilder();
    sb.append("\\\"task\\\":{");
    sb.append(" \\\"type\\\":\\\"");
    sb.append(curCommponentName);
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

  public static void addQuicklink(Service serviceSpec, String label,
      String link) {
    Map<String, String> quicklinks = serviceSpec.getQuicklinks();
    if (null == quicklinks) {
      quicklinks = new HashMap<>();
      serviceSpec.setQuicklinks(quicklinks);
    }

    if (SubmarineLogs.isVerbose()) {
      LOG.info("Added quicklink, " + label + "=" + link);
    }

    quicklinks.put(label, link);
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
