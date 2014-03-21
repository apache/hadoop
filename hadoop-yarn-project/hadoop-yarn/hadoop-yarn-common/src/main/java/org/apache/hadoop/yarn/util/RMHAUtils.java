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

package org.apache.hadoop.yarn.util;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.HAServiceProtocol.HAServiceState;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.yarn.client.RMHAServiceTarget;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

@Private
@Unstable
public class RMHAUtils {

  public static String findActiveRMHAId(YarnConfiguration conf) {
    YarnConfiguration yarnConf = new YarnConfiguration(conf);
    Collection<String> rmIds =
        yarnConf.getStringCollection(YarnConfiguration.RM_HA_IDS);
    for (String currentId : rmIds) {
      yarnConf.set(YarnConfiguration.RM_HA_ID, currentId);
      try {
        HAServiceState haState = getHAState(yarnConf);
        if (haState.equals(HAServiceState.ACTIVE)) {
          return currentId;
        }
      } catch (Exception e) {
        // Couldn't check if this RM is active. Do nothing. Worst case,
        // we wouldn't find an Active RM and return null.
      }
    }
    return null; // Couldn't find an Active RM
  }

  private static HAServiceState getHAState(YarnConfiguration yarnConf)
      throws Exception {
    HAServiceTarget haServiceTarget;
    int rpcTimeoutForChecks =
        yarnConf.getInt(CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_KEY,
            CommonConfigurationKeys.HA_FC_CLI_CHECK_TIMEOUT_DEFAULT);

    yarnConf.set(CommonConfigurationKeys.HADOOP_SECURITY_SERVICE_USER_NAME_KEY,
        yarnConf.get(YarnConfiguration.RM_PRINCIPAL, ""));
    haServiceTarget = new RMHAServiceTarget(yarnConf);
    HAServiceProtocol proto =
        haServiceTarget.getProxy(yarnConf, rpcTimeoutForChecks);
    HAServiceState haState = proto.getServiceStatus().getState();
    return haState;
  }

  public static List<String> getRMHAWebappAddresses(
      final YarnConfiguration conf) {
    Collection<String> rmIds =
        conf.getStringCollection(YarnConfiguration.RM_HA_IDS);
    List<String> addrs = new ArrayList<String>();
    if (YarnConfiguration.useHttps(conf)) {
      for (String id : rmIds) {
        String addr = conf.get(
            YarnConfiguration.RM_WEBAPP_HTTPS_ADDRESS + "." + id);
        if (addr != null) {
          addrs.add(addr);
        }
      }
    } else {
      for (String id : rmIds) {
        String addr = conf.get(YarnConfiguration.RM_WEBAPP_ADDRESS + "." + id);
        if (addr != null) {
          addrs.add(addr);
        }
      }
    }
    return addrs;
  }
}