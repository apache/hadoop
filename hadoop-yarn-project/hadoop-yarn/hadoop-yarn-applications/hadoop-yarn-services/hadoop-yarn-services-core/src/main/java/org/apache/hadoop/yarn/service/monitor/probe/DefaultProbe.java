/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.service.monitor.probe;

import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.service.component.instance.ComponentInstance;
import org.apache.hadoop.yarn.service.utils.ServiceRegistryUtils;
import org.apache.hadoop.yarn.service.utils.ServiceUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A probe that checks whether the AM has retrieved an IP for a container.
 * Optional parameters enable a subsequent check for whether a DNS lookup can
 * be performed for the container's hostname. Configurable properties include:
 *
 *   dns.check.enabled - true if DNS check should be performed (default false)
 *   dns.address - optional IP:port address of DNS server to use for DNS check
 */
public class DefaultProbe extends Probe {
  private final boolean dnsCheckEnabled;
  private final String dnsAddress;

  public DefaultProbe(Map<String, String> props) {
    this("Default probe: IP presence", props);
  }

  protected DefaultProbe(String name, Map<String, String> props) {
    this.dnsCheckEnabled = getPropertyBool(props,
        DEFAULT_PROBE_DNS_CHECK_ENABLED,
        DEFAULT_PROBE_DNS_CHECK_ENABLED_DEFAULT);
    this.dnsAddress = props.get(DEFAULT_PROBE_DNS_ADDRESS);
    String additionalName = "";
    if (dnsCheckEnabled) {
      if (dnsAddress == null) {
        additionalName = " with DNS checking";
      } else {
        additionalName =  " with DNS checking and DNS server address " +
            dnsAddress;
      }
    }
    setName(name + additionalName);
  }

  public static DefaultProbe create() throws IOException {
    return new DefaultProbe(Collections.emptyMap());
  }

  public static DefaultProbe create(Map<String, String> props) throws
      IOException {
    return new DefaultProbe(props);
  }

  @Override
  public ProbeStatus ping(ComponentInstance instance) {
    ProbeStatus status = new ProbeStatus();

    ContainerStatus containerStatus = instance.getContainerStatus();
    if (containerStatus == null || ServiceUtils.isEmpty(containerStatus
        .getIPs())) {
      status.fail(this, new IOException(
          instance.getCompInstanceName() + ": IP is not available yet"));
      return status;
    }

    String hostname = instance.getHostname();
    if (dnsCheckEnabled && !ServiceRegistryUtils.registryDNSLookupExists(
        dnsAddress, hostname)) {
      status.fail(this, new IOException(
          instance.getCompInstanceName() + ": DNS checking is enabled, but " +
              "lookup for " + hostname + " is not available yet"));
      return status;
    }

    status.succeed(this);
    return status;
  }

  protected boolean isDnsCheckEnabled() {
    return dnsCheckEnabled;
  }
}
