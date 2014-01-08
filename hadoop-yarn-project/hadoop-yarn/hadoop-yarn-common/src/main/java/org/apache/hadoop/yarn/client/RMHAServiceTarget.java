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

package org.apache.hadoop.yarn.client;

import org.apache.hadoop.ha.BadFencingConfigurationException;
import org.apache.hadoop.ha.HAServiceTarget;
import org.apache.hadoop.ha.NodeFencer;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;

public class RMHAServiceTarget extends HAServiceTarget {
  private final boolean autoFailoverEnabled;
  private final InetSocketAddress haAdminServiceAddress;

  public RMHAServiceTarget(YarnConfiguration conf)
      throws IOException {
    autoFailoverEnabled = HAUtil.isAutomaticFailoverEnabled(conf);
    haAdminServiceAddress = conf.getSocketAddr(
        YarnConfiguration.RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_ADDRESS,
        YarnConfiguration.DEFAULT_RM_ADMIN_PORT);
  }

  @Override
  public InetSocketAddress getAddress() {
    return haAdminServiceAddress;
  }

  @Override
  public InetSocketAddress getZKFCAddress() {
    // TODO (YARN-1177): ZKFC implementation
    throw new UnsupportedOperationException("RMHAServiceTarget doesn't have " +
        "a corresponding ZKFC address");
  }

  @Override
  public NodeFencer getFencer() {
    return null;
  }

  @Override
  public void checkFencingConfigured() throws BadFencingConfigurationException {
    throw new BadFencingConfigurationException("Fencer not configured");
  }

  @Override
  public boolean isAutoFailoverEnabled() {
    return autoFailoverEnabled;
  }
}
