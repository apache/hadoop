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
package org.apache.hadoop.net;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.net.URI;

/**
 * This class creates the DomainNameResolver instance based on the config.
 * It can either create the default resolver for the whole resolving for
 * hadoop or create individual resolver per nameservice or yarn.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class DomainNameResolverFactory {

  private DomainNameResolverFactory() {
    // Utility classes should not have a public or default constructor
  }

  /**
   * Create a domain name resolver to convert the domain name in the config to
   * the actual IP addresses of the Namenode/Router/RM.
   *
   * @param conf Configuration to get the resolver from.
   * @param uri the url that the resolver will be used against
   * @param configKey The config key name suffixed with
   *                  the nameservice/yarnservice.
   * @return Domain name resolver.
   */
  public static DomainNameResolver newInstance(
      Configuration conf, URI uri, String configKey) throws IOException {
    String host = uri.getHost();
    String confKeyWithHost = configKey + "." + host;
    return newInstance(conf, confKeyWithHost);
  }

  /**
   * This function gets the instance based on the config.
   *
   * @param conf Configuration
   * @param configKey config key name.
   * @return Domain name resolver.
   * @throws IOException when the class cannot be found or initiated.
   */
  public static DomainNameResolver newInstance(
      Configuration conf, String configKey) {
    Class<? extends DomainNameResolver> resolverClass = conf.getClass(
        configKey,
        DNSDomainNameResolver.class,
        DomainNameResolver.class);
    return ReflectionUtils.newInstance(resolverClass, conf);
  }
}
