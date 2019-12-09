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

package org.apache.hadoop.yarn.service.utils;

import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.yarn.service.conf.YarnServiceConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.naming.Context;
import javax.naming.NameNotFoundException;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Hashtable;


public class ServiceRegistryUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(ServiceRegistryUtils.class);

  public static final String SVC_USERS = "/services/yarn/users";

  /**
   * Get the registry path for an instance under the user's home node
   * @param instanceName application instance
   * @return a path to the registry location for this application instance.
   */
  public static String registryPathForInstance(String instanceName) {
    return RegistryUtils.servicePath(
        RegistryUtils.currentUser(), YarnServiceConstants.APP_TYPE, instanceName
    );
  }

  /**
 * Build the path to a service folder
 * @param username user name
 * @param serviceName service name
 * @return the home path to the service
 */
  public static String mkServiceHomePath(String username, String serviceName) {
    return mkUserHomePath(username) + "/" + serviceName;
  }

  /**
   * Build the path to a user home folder;
   */
  public static String mkUserHomePath(String username) {
    return SVC_USERS + "/" + username;
  }

  /**
   * Determine whether a DNS lookup exists for a given name. If a DNS server
   * address is provided, the lookup will be performed against this DNS
   * server. This option is provided because it may be desirable to perform
   * the lookup against Registry DNS directly to avoid caching of negative
   * responses that may be performed by other DNS servers, thereby allowing the
   * lookup to succeed sooner.
   *
   * @param addr host:port dns address, or null
   * @param name name to look up
   * @return true if a lookup succeeds for the specified name
   */
  public static boolean registryDNSLookupExists(String addr, String
      name) {
    if (addr == null) {
      try {
        InetAddress.getByName(name);
        return true;
      } catch (UnknownHostException e) {
        return false;
      }
    }

    String dnsURI = String.format("dns://%s", addr);
    Hashtable<String, Object> env = new Hashtable<>();
    env.put(Context.INITIAL_CONTEXT_FACTORY,
        "com.sun.jndi.dns.DnsContextFactory");
    env.put(Context.PROVIDER_URL, dnsURI);

    try {
      DirContext ictx = new InitialDirContext(env);
      Attributes attrs = ictx.getAttributes(name, new String[]{"A"});

      if (attrs.size() > 0) {
        return true;
      }
    } catch (NameNotFoundException e) {
      // this doesn't need to be logged
    } catch (NamingException e) {
      LOG.error("Got exception when performing DNS lookup", e);
    }

    return false;
  }

}
