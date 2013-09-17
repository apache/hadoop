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

package org.apache.hadoop.lib.service.security;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.lib.lang.XException;
import org.apache.hadoop.lib.server.BaseService;
import org.apache.hadoop.lib.server.ServiceException;
import org.apache.hadoop.lib.service.Groups;
import org.apache.hadoop.lib.service.ProxyUser;
import org.apache.hadoop.lib.util.Check;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.security.AccessControlException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@InterfaceAudience.Private
public class ProxyUserService extends BaseService implements ProxyUser {
  private static Logger LOG = LoggerFactory.getLogger(ProxyUserService.class);

  @InterfaceAudience.Private
  public static enum ERROR implements XException.ERROR {
    PRXU01("Could not normalize host name [{0}], {1}"),
    PRXU02("Missing [{0}] property");

    private String template;

    ERROR(String template) {
      this.template = template;
    }

    @Override
    public String getTemplate() {
      return template;
    }
  }

  private static final String PREFIX = "proxyuser";
  private static final String GROUPS = ".groups";
  private static final String HOSTS = ".hosts";

  private Map<String, Set<String>> proxyUserHosts = new HashMap<String, Set<String>>();
  private Map<String, Set<String>> proxyUserGroups = new HashMap<String, Set<String>>();

  public ProxyUserService() {
    super(PREFIX);
  }

  @Override
  public Class getInterface() {
    return ProxyUser.class;
  }

  @Override
  public Class[] getServiceDependencies() {
    return new Class[]{Groups.class};
  }

  @Override
  protected void init() throws ServiceException {
    for (Map.Entry<String, String> entry : getServiceConfig()) {
      String key = entry.getKey();
      if (key.endsWith(GROUPS)) {
        String proxyUser = key.substring(0, key.lastIndexOf(GROUPS));
        if (getServiceConfig().get(proxyUser + HOSTS) == null) {
          throw new ServiceException(ERROR.PRXU02, getPrefixedName(proxyUser + HOSTS));
        }
        String value = entry.getValue().trim();
        LOG.info("Loading proxyuser settings [{}]=[{}]", key, value);
        Set<String> values = null;
        if (!value.equals("*")) {
          values = new HashSet<String>(Arrays.asList(value.split(",")));
        }
        proxyUserGroups.put(proxyUser, values);
      }
      if (key.endsWith(HOSTS)) {
        String proxyUser = key.substring(0, key.lastIndexOf(HOSTS));
        if (getServiceConfig().get(proxyUser + GROUPS) == null) {
          throw new ServiceException(ERROR.PRXU02, getPrefixedName(proxyUser + GROUPS));
        }
        String value = entry.getValue().trim();
        LOG.info("Loading proxyuser settings [{}]=[{}]", key, value);
        Set<String> values = null;
        if (!value.equals("*")) {
          String[] hosts = value.split(",");
          for (int i = 0; i < hosts.length; i++) {
            String originalName = hosts[i];
            try {
              hosts[i] = normalizeHostname(originalName);
            } catch (Exception ex) {
              throw new ServiceException(ERROR.PRXU01, originalName, ex.getMessage(), ex);
            }
            LOG.info("  Hostname, original [{}], normalized [{}]", originalName, hosts[i]);
          }
          values = new HashSet<String>(Arrays.asList(hosts));
        }
        proxyUserHosts.put(proxyUser, values);
      }
    }
  }

  @Override
  public void validate(String proxyUser, String proxyHost, String doAsUser) throws IOException,
    AccessControlException {
    Check.notEmpty(proxyUser, "proxyUser");
    Check.notEmpty(proxyHost, "proxyHost");
    Check.notEmpty(doAsUser, "doAsUser");
    LOG.debug("Authorization check proxyuser [{}] host [{}] doAs [{}]",
              new Object[]{proxyUser, proxyHost, doAsUser});
    if (proxyUserHosts.containsKey(proxyUser)) {
      proxyHost = normalizeHostname(proxyHost);
      validateRequestorHost(proxyUser, proxyHost, proxyUserHosts.get(proxyUser));
      validateGroup(proxyUser, doAsUser, proxyUserGroups.get(proxyUser));
    } else {
      throw new AccessControlException(MessageFormat.format("User [{0}] not defined as proxyuser", proxyUser));
    }
  }

  private void validateRequestorHost(String proxyUser, String hostname, Set<String> validHosts)
    throws IOException, AccessControlException {
    if (validHosts != null) {
      if (!validHosts.contains(hostname) && !validHosts.contains(normalizeHostname(hostname))) {
        throw new AccessControlException(MessageFormat.format("Unauthorized host [{0}] for proxyuser [{1}]",
                                                              hostname, proxyUser));
      }
    }
  }

  private void validateGroup(String proxyUser, String user, Set<String> validGroups) throws IOException,
    AccessControlException {
    if (validGroups != null) {
      List<String> userGroups = getServer().get(Groups.class).getGroups(user);
      for (String g : validGroups) {
        if (userGroups.contains(g)) {
          return;
        }
      }
      throw new AccessControlException(
        MessageFormat.format("Unauthorized proxyuser [{0}] for user [{1}], not in proxyuser groups",
                             proxyUser, user));
    }
  }

  private String normalizeHostname(String name) {
    try {
      InetAddress address = InetAddress.getByName(name);
      return address.getCanonicalHostName();
    } catch (IOException ex) {
      throw new AccessControlException(MessageFormat.format("Could not resolve host [{0}], {1}", name,
                                                            ex.getMessage()));
    }
  }

}
