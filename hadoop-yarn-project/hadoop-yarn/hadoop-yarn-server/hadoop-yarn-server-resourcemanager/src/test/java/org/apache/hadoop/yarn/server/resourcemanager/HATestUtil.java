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
package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.ServerSocketUtil;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;

public class HATestUtil {

  public static void setRpcAddressForRM(String rmId, int base,
      Configuration conf) throws IOException {
    for (String confKey : YarnConfiguration.getServiceAddressConfKeys(conf)) {
      setConfForRM(rmId, confKey, "0.0.0.0:" + ServerSocketUtil.getPort(base +
          YarnConfiguration.getRMDefaultPortNumber(confKey, conf), 10), conf);
    }
  }

  public static void setConfForRM(String rmId, String prefix, String value,
      Configuration conf) {
    conf.set(HAUtil.addSuffix(prefix, rmId), value);
  }
}
