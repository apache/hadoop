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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.zookeeper.data.ACL;

import java.util.Collections;
import java.util.List;

/**
 * Helper class that provides utility methods specific to ZK operations
 */
@InterfaceAudience.Private
public class RMZKUtils {
  private static final Log LOG = LogFactory.getLog(RMZKUtils.class);

  /**
   * Utility method to fetch the ZK ACLs from the configuration
   */
  public static List<ACL> getZKAcls(Configuration conf) throws Exception {
    // Parse authentication from configuration.
    String zkAclConf =
        conf.get(YarnConfiguration.RM_ZK_ACL,
            YarnConfiguration.DEFAULT_RM_ZK_ACL);
    try {
      zkAclConf = ZKUtil.resolveConfIndirection(zkAclConf);
      return ZKUtil.parseACLs(zkAclConf);
    } catch (Exception e) {
      LOG.error("Couldn't read ACLs based on " + YarnConfiguration.RM_ZK_ACL);
      throw e;
    }
  }

  /**
   * Utility method to fetch ZK auth info from the configuration
   */
  public static List<ZKUtil.ZKAuthInfo> getZKAuths(Configuration conf)
      throws Exception {
    String zkAuthConf = conf.get(YarnConfiguration.RM_ZK_AUTH);
    try {
      zkAuthConf = ZKUtil.resolveConfIndirection(zkAuthConf);
      if (zkAuthConf != null) {
        return ZKUtil.parseAuth(zkAuthConf);
      } else {
        return Collections.emptyList();
      }
    } catch (Exception e) {
      LOG.error("Couldn't read Auth based on " + YarnConfiguration.RM_ZK_AUTH);
      throw e;
    }
  }
}
