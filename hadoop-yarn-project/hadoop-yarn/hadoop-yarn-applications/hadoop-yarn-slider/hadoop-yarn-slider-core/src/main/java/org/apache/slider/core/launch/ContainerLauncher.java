/*
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

package org.apache.slider.core.launch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.security.ContainerTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.slider.common.tools.CoreFileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;

/**
 * Code to ease launching of any container
 */
public class ContainerLauncher extends AbstractLauncher {
  private static final Logger log =
    LoggerFactory.getLogger(ContainerLauncher.class);
  // Allocated container
  public final Container container;

  public ContainerLauncher(Configuration conf,
      CoreFileSystem coreFileSystem,
      Container container,
      Credentials credentials) {
    super(conf, coreFileSystem, credentials);
    this.container = container;
  }

  /**
   * This code is in the dist shell examples -it's been moved here
   * so that if it is needed, it's still here
   * @return a remote user with a token to access the container.
   */
  public UserGroupInformation setupUGI() {
    UserGroupInformation user =
      UserGroupInformation.createRemoteUser(container.getId().toString());
    String cmIpPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort();
    final InetSocketAddress cmAddress = NetUtils.createSocketAddr(cmIpPortStr);

    org.apache.hadoop.yarn.api.records.Token containerToken = container.getContainerToken();
    if (containerToken != null) {
      Token<ContainerTokenIdentifier> token =
        ConverterUtils.convertFromYarn(containerToken, cmAddress);
      user.addToken(token);
    }
    return user;
  }

}
