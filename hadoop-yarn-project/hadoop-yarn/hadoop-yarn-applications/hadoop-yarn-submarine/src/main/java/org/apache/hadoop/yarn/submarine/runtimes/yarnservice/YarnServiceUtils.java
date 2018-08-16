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
import org.apache.hadoop.yarn.service.client.ServiceClient;
import org.apache.hadoop.yarn.submarine.common.Envs;

public class YarnServiceUtils {
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

  public static String getTFConfigEnv(String curCommponentName, int nWorkers,
      int nPs, String serviceName, String userName, String domain) {
    String commonEndpointSuffix =
        "." + serviceName + "." + userName + "." + domain + ":8000";

    String json = "{\\\"cluster\\\":{";

    String master = getComponentArrayJson("master", 1, commonEndpointSuffix)
        + ",";
    String worker = getComponentArrayJson("worker", nWorkers - 1,
        commonEndpointSuffix) + ",";
    String ps = getComponentArrayJson("ps", nPs, commonEndpointSuffix) + "},";

    String task =
        "\\\"task\\\":{" + " \\\"type\\\":\\\"" + curCommponentName + "\\\","
            + " \\\"index\\\":" + '$' + Envs.TASK_INDEX_ENV + "},";
    String environment = "\\\"environment\\\":\\\"cloud\\\"}";

    return json + master + worker + ps + task + environment;
  }

  private static String getComponentArrayJson(String componentName, int count,
      String endpointSuffix) {
    String component = "\\\"" + componentName + "\\\":";
    String array = "[";
    for (int i = 0; i < count; i++) {
      array = array + "\\\"" + componentName + "-" + i
          + endpointSuffix + "\\\"";
      if (i != count - 1) {
        array = array + ",";
      }
    }
    array = array + "]";
    return component + array;
  }
}
