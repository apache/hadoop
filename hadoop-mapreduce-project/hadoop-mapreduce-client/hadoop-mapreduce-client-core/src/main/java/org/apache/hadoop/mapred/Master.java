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

package org.apache.hadoop.mapred;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.yarn.client.util.YarnClientUtils;

@Private
@Unstable
public class Master {
  public enum State {
    INITIALIZING, RUNNING;
  }

  public static String getMasterAddress(Configuration conf) {
    String masterAddress = conf.get(MRConfig.MASTER_ADDRESS, "localhost:8012");

    return NetUtils.createSocketAddr(masterAddress, 8012,
            MRConfig.MASTER_ADDRESS).getHostName();
  }

  public static String getMasterPrincipal(Configuration conf)
      throws IOException {
    String masterPrincipal;
    String framework = conf.get(MRConfig.FRAMEWORK_NAME,
            MRConfig.YARN_FRAMEWORK_NAME);

    if (framework.equals(MRConfig.CLASSIC_FRAMEWORK_NAME)) {
      String masterAddress = getMasterAddress(conf);
      // get kerberos principal for use as delegation token renewer
      masterPrincipal =
          SecurityUtil.getServerPrincipal(conf.get(MRConfig.MASTER_USER_NAME),
          masterAddress);
    } else {
      masterPrincipal = YarnClientUtils.getRmPrincipal(conf);
    }

    return masterPrincipal;
  }
}
