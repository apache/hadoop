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

package org.apache.hadoop.registry.client.api;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.server.dns.RegistryDNS;

/**
 * A factory for DNS operation service instances.
 */
public final class DNSOperationsFactory implements RegistryConstants {

  /**
   * DNS Implementation type.
   */
  public enum DNSImplementation {
    DNSJAVA
  }

  private DNSOperationsFactory() {
  }

  /**
   * Create and initialize a DNS operations instance.
   *
   * @param conf configuration
   * @return a DNS operations instance
   */
  public static DNSOperations createInstance(Configuration conf) {
    return createInstance("DNSOperations", DNSImplementation.DNSJAVA, conf);
  }

  /**
   * Create and initialize a registry operations instance.
   * Access rights will be determined from the configuration.
   *
   * @param name name of the instance
   * @param impl the DNS implementation.
   * @param conf configuration
   * @return a registry operations instance
   */
  public static DNSOperations createInstance(String name,
      DNSImplementation impl,
      Configuration conf) {
    Preconditions.checkArgument(conf != null, "Null configuration");
    DNSOperations operations = null;
    switch (impl) {
    case DNSJAVA:
      operations = new RegistryDNS(name);
      break;

    default:
      throw new IllegalArgumentException(
          String.format("%s is not available", impl.toString()));
    }

    //operations.init(conf);
    return operations;
  }

}
