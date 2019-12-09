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

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.binding.RegistryPathUtils;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.impl.zk.RegistryInternalConstants;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.hadoop.registry.client.binding.RegistryPathUtils.encodeForRegistry;
import static org.apache.hadoop.registry.client.binding.RegistryUtils.convertUsername;
import static org.apache.hadoop.registry.client.binding.RegistryUtils.getCurrentUsernameUnencoded;
import static org.apache.hadoop.registry.client.binding.RegistryUtils.servicePath;

/**
 * Generic code to get the URLs for clients via the registry
 */
public class ClientRegistryBinder {
  private static final Logger log =
      LoggerFactory.getLogger(ClientRegistryBinder.class);

  private final RegistryOperations operations;

  public ClientRegistryBinder(RegistryOperations operations) {
    this.operations = operations;
  }

  /**
   * Buld the user path -switches to the system path if the user is "".
   * It also cross-converts the username to ascii via punycode
   * @param username username or ""
   * @return the path to the user
   */
  public static String homePathForUser(String username) {
    Preconditions.checkArgument(username != null, "null user");

    // catch recursion
    if (username.startsWith(RegistryConstants.PATH_USERS)) {
      return username;
    }

    if (username.isEmpty()) {
      return RegistryConstants.PATH_SYSTEM_SERVICES;
    }

    // convert username to registry name
    String convertedName = convertUsername(username);

    return RegistryPathUtils.join(RegistryConstants.PATH_USERS,
        encodeForRegistry(convertedName));
  }

  /**
   * Get the current username, before any encoding has been applied.
   * @return the current user from the kerberos identity, falling back
   * to the user and/or env variables.
   */
  public static String currentUsernameUnencoded() {
    String env_hadoop_username = System.getenv(
        RegistryInternalConstants.HADOOP_USER_NAME);
    return getCurrentUsernameUnencoded(env_hadoop_username);
  }

  /**
   * Qualify a user.
   * <ol>
   *   <li> <code>"~"</code> maps to user home path home</li>
   *   <li> <code>"~user"</code> maps to <code>/users/$user</code></li>
   *   <li> <code>"/"</code> maps to <code>/services/</code></li>
   * </ol>
   * @param user the username
   * @return the base path
   */
  public static String qualifyUser(String user) {
    // qualify the user
    String t = user.trim();
    if (t.startsWith("/")) {
      // already resolved
      return t;
    } else if (t.equals("~")) {
      // self
      return currentUsernameUnencoded();
    } else if (t.startsWith("~")) {
      // another user
      // convert username to registry name
      String convertedName = convertUsername(t.substring(1));

      return RegistryPathUtils.join(RegistryConstants.PATH_USERS,
          encodeForRegistry(convertedName));
    } else {
      return "/" + t;
    }
  }

  /**
   * Look up an external REST API
   * @param user user which will be qualified as per {@link #qualifyUser(String)}
   * @param serviceClass service class
   * @param instance instance name
   * @param api API
   * @return the API, or an exception is raised.
   * @throws IOException
   */
  public String lookupExternalRestAPI(String user,
      String serviceClass,
      String instance,
      String api)
      throws IOException {
    String qualified = qualifyUser(user);
    String path = servicePath(qualified, serviceClass, instance);
    String restAPI = resolveExternalRestAPI(api, path);
    if (restAPI == null) {
      throw new PathNotFoundException(path + " API " + api);
    }
    return restAPI;
  }

  /**
   * Resolve a service record then return an external REST API exported it.
   *
   * @param api API to resolve
   * @param path path of the service record
   * @return null if the record exists but the API is absent or it has no
   * REST endpoints.
   * @throws IOException resolution problems, as covered in
   * {@link RegistryOperations#resolve(String)}
   */
  protected String resolveExternalRestAPI(String api, String path) throws
      IOException {
    ServiceRecord record = operations.resolve(path);
    return lookupRestAPI(record, api, true);
  }

  /**
   * Look up an external REST API endpoint
   * @param record service record
   * @param api URI of api
   * @param external flag to indicate this is an external record
   * @return the first endpoint of the implementation, or null if there
   * is no entry for the API, implementation or it's the wrong type.
   */
  public static String lookupRestAPI(ServiceRecord record,
      String api, boolean external) throws InvalidRecordException {
    try {
      String url = null;
      Endpoint endpoint = getEndpoint(record, api, external);
      List<String> addresses =
          RegistryTypeUtils.retrieveAddressesUriType(endpoint);
      if (addresses != null && !addresses.isEmpty()) {
        url = addresses.get(0);
      }
      return url;
    } catch (InvalidRecordException e) {
      log.debug("looking for API {}", api, e);
      return null;
    }
  }

  /**
   * Get an endpont by API
   * @param record service record
   * @param api API
   * @param external flag to indicate this is an external record
   * @return the endpoint or null
   */
  public static Endpoint getEndpoint(ServiceRecord record,
      String api,
      boolean external) {
    return external ? record.getExternalEndpoint(api)
                    : record.getInternalEndpoint(api);
  }


}
