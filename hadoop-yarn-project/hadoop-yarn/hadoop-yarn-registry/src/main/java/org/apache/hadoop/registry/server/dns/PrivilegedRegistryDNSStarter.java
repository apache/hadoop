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
package org.apache.hadoop.registry.server.dns;

import org.apache.commons.daemon.Daemon;
import org.apache.commons.daemon.DaemonContext;
import org.apache.hadoop.registry.client.api.DNSOperationsFactory;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.registry.client.api.RegistryConstants.DEFAULT_DNS_PORT;
import static org.apache.hadoop.registry.client.api.RegistryConstants.KEY_DNS_PORT;

/**
 * This class is used to allow the RegistryDNSServer to run on a privileged
 * port (e.g. 53).
 */
public class PrivilegedRegistryDNSStarter implements Daemon {
  private static final Logger LOG =
      LoggerFactory.getLogger(PrivilegedRegistryDNSStarter.class);

  private YarnConfiguration conf;
  private RegistryDNS registryDNS;
  private RegistryDNSServer registryDNSServer;

  @Override
  public void init(DaemonContext context) throws Exception {
    String[] args = context.getArguments();
    StringUtils.startupShutdownMessage(RegistryDNSServer.class, args, LOG);
    conf = new YarnConfiguration();
    new GenericOptionsParser(conf, args);

    int port = conf.getInt(KEY_DNS_PORT, DEFAULT_DNS_PORT);
    if (port < 1 || port > 1023) {
      throw new RuntimeException("Must start privileged registry DNS server " +
          "with '" + KEY_DNS_PORT + "' configured to a privileged port.");
    }

    try {
      registryDNS = (RegistryDNS) DNSOperationsFactory.createInstance(conf);
      registryDNS.initializeChannels(conf);
    } catch (Exception e) {
      LOG.error("Error initializing Registry DNS", e);
      throw e;
    }
  }

  @Override
  public void start() throws Exception {
    registryDNSServer = RegistryDNSServer.launchDNSServer(conf, registryDNS);
  }

  @Override
  public void stop() throws Exception {
  }

  @Override
  public void destroy() {
    registryDNSServer.stop();
  }

}
