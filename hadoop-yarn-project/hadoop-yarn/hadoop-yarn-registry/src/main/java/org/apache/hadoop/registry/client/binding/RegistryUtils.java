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

package org.apache.hadoop.registry.client.binding;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.PathNotFoundException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.exceptions.InvalidPathnameException;
import org.apache.hadoop.registry.client.exceptions.InvalidRecordException;
import org.apache.hadoop.registry.client.exceptions.NoRecordException;
import org.apache.hadoop.registry.client.impl.zk.RegistryInternalConstants;
import org.apache.hadoop.registry.client.types.RegistryPathStatus;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.registry.client.binding.RegistryPathUtils.*;

import java.io.EOFException;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Utility methods for working with a registry.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class RegistryUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryUtils.class);

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
   * Convert the username to that which can be used for registry
   * entries. Lower cases it,
   * Strip the kerberos realm off a username if needed, and any "/" hostname
   * entries
   * @param username user
   * @return the converted username
   */
  public static String convertUsername(String username) {
    String converted =
        org.apache.hadoop.util.StringUtils.toLowerCase(username);
    int atSymbol = converted.indexOf('@');
    if (atSymbol > 0) {
      converted = converted.substring(0, atSymbol);
    }
    int slashSymbol = converted.indexOf('/');
    if (slashSymbol > 0) {
      converted = converted.substring(0, slashSymbol);
    }
    return converted;
  }

  /**
   * Create a service classpath
   * @param user username or ""
   * @param serviceClass service name
   * @return a full path
   */
  public static String serviceclassPath(String user,
      String serviceClass) {
    String services = join(homePathForUser(user),
        RegistryConstants.PATH_USER_SERVICES);
    return join(services,
        serviceClass);
  }

  /**
   * Create a path to a service under a user and service class
   * @param user username or ""
   * @param serviceClass service name
   * @param serviceName service name unique for that user and service class
   * @return a full path
   */
  public static String servicePath(String user,
      String serviceClass,
      String serviceName) {

    return join(
        serviceclassPath(user, serviceClass),
        serviceName);
  }

  /**
   * Create a path for listing components under a service
   * @param user username or ""
   * @param serviceClass service name
   * @param serviceName service name unique for that user and service class
   * @return a full path
   */
  public static String componentListPath(String user,
      String serviceClass, String serviceName) {

    return join(servicePath(user, serviceClass, serviceName),
        RegistryConstants.SUBPATH_COMPONENTS);
  }

  /**
   * Create the path to a service record for a component
   * @param user username or ""
   * @param serviceClass service name
   * @param serviceName service name unique for that user and service class
   * @param componentName unique name/ID of the component
   * @return a full path
   */
  public static String componentPath(String user,
      String serviceClass, String serviceName, String componentName) {

    return join(
        componentListPath(user, serviceClass, serviceName),
        componentName);
  }

  /**
   * List service records directly under a path
   * @param registryOperations registry operations instance
   * @param path path to list
   * @return a mapping of the service records that were resolved, indexed
   * by their full path
   * @throws IOException
   */
  public static Map<String, ServiceRecord> listServiceRecords(
      RegistryOperations registryOperations,
      String path) throws IOException {
    Map<String, RegistryPathStatus> children =
        statChildren(registryOperations, path);
    return extractServiceRecords(registryOperations,
        path,
        children.values());
  }

  /**
   * List children of a directory and retrieve their
   * {@link RegistryPathStatus} values.
   * <p>
   * This is not an atomic operation; A child may be deleted
   * during the iteration through the child entries. If this happens,
   * the <code>PathNotFoundException</code> is caught and that child
   * entry ommitted.
   *
   * @param path path
   * @return a possibly empty map of child entries listed by
   * their short name.
   * @throws PathNotFoundException path is not in the registry.
   * @throws InvalidPathnameException the path is invalid.
   * @throws IOException Any other IO Exception
   */
  public static Map<String, RegistryPathStatus> statChildren(
      RegistryOperations registryOperations,
      String path)
      throws PathNotFoundException,
      InvalidPathnameException,
      IOException {
    List<String> childNames = registryOperations.list(path);
    Map<String, RegistryPathStatus> results =
        new HashMap<String, RegistryPathStatus>();
    for (String childName : childNames) {
      String child = join(path, childName);
      try {
        RegistryPathStatus stat = registryOperations.stat(child);
        results.put(childName, stat);
      } catch (PathNotFoundException pnfe) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("stat failed on {}: moved? {}", child, pnfe, pnfe);
        }
        // and continue
      }
    }
    return results;
  }

  /**
   * Get the home path of the current user.
   * <p>
   *  In an insecure cluster, the environment variable
   *  <code>HADOOP_USER_NAME</code> is queried <i>first</i>.
   * <p>
   * This means that in a YARN container where the creator set this
   * environment variable to propagate their identity, the defined
   * user name is used in preference to the actual user.
   * <p>
   * In a secure cluster, the kerberos identity of the current user is used.
   * @return a path for the current user's home dir.
   * @throws RuntimeException if the current user identity cannot be determined
   * from the OS/kerberos.
   */
  public static String homePathForCurrentUser() {
    String shortUserName = currentUsernameUnencoded();
    return homePathForUser(shortUserName);
  }

  /**
   * Get the current username, before any encoding has been applied.
   * @return the current user from the kerberos identity, falling back
   * to the user and/or env variables.
   */
  private static String currentUsernameUnencoded() {
    String env_hadoop_username = System.getenv(
        RegistryInternalConstants.HADOOP_USER_NAME);
    return getCurrentUsernameUnencoded(env_hadoop_username);
  }

  /**
   * Get the current username, using the value of the parameter
   * <code>env_hadoop_username</code> if it is set on an insecure cluster.
   * This ensures that the username propagates correctly across processes
   * started by YARN.
   * <p>
   * This method is primarly made visible for testing.
   * @param env_hadoop_username the environment variable
   * @return the selected username
   * @throws RuntimeException if there is a problem getting the short user
   * name of the current user.
   */
  @VisibleForTesting
  public static String getCurrentUsernameUnencoded(String env_hadoop_username) {
    String shortUserName = null;
    if (!UserGroupInformation.isSecurityEnabled()) {
      shortUserName = env_hadoop_username;
    }
    if (StringUtils.isEmpty(shortUserName)) {
      try {
        shortUserName = UserGroupInformation.getCurrentUser().getShortUserName();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    return shortUserName;
  }

  /**
   * Get the current user path formatted for the registry
   * <p>
   *  In an insecure cluster, the environment variable
   *  <code>HADOOP_USER_NAME </code> is queried <i>first</i>.
   * <p>
   * This means that in a YARN container where the creator set this
   * environment variable to propagate their identity, the defined
   * user name is used in preference to the actual user.
   * <p>
   * In a secure cluster, the kerberos identity of the current user is used.
   * @return the encoded shortname of the current user
   * @throws RuntimeException if the current user identity cannot be determined
   * from the OS/kerberos.
   *
   */
  public static String currentUser() {
    String shortUserName = currentUsernameUnencoded();
    return registryUser(shortUserName);
  }

  /**
   * Convert the given user name formatted for the registry.
   *
   * @param shortUserName
   * @return converted user name
   */
  public static String registryUser(String shortUserName) {
    String encodedName =  encodeForRegistry(shortUserName);
    // DNS name doesn't allow "_", replace it with "-"
    encodedName = RegistryUtils.convertUsername(encodedName);
    return encodedName.replace("_", "-");
  }

  /**
   * Extract all service records under a list of stat operations...this
   * skips entries that are too short or simply not matching
   * @param operations operation support for fetches
   * @param parentpath path of the parent of all the entries
   * @param stats Collection of stat results
   * @return a possibly empty map of fullpath:record.
   * @throws IOException for any IO Operation that wasn't ignored.
   */
  public static Map<String, ServiceRecord> extractServiceRecords(
      RegistryOperations operations,
      String parentpath,
      Collection<RegistryPathStatus> stats) throws IOException {
    Map<String, ServiceRecord> results = new HashMap<String, ServiceRecord>(stats.size());
    for (RegistryPathStatus stat : stats) {
      if (stat.size > ServiceRecord.RECORD_TYPE.length()) {
        // maybe has data
        String path = join(parentpath, stat.path);
        try {
          ServiceRecord serviceRecord = operations.resolve(path);
          results.put(path, serviceRecord);
        } catch (EOFException ignored) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("data too short for {}", path);
          }
        } catch (InvalidRecordException record) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Invalid record at {}", path);
          }
        } catch (NoRecordException record) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("No record at {}", path);
          }
        }
      }
    }
    return results;
  }

  /**
   * Extract all service records under a list of stat operations...this
   * non-atomic action skips entries that are too short or simply not matching.
   * <p>
   * @param operations operation support for fetches
   * @param parentpath path of the parent of all the entries
   * @return a possibly empty map of fullpath:record.
   * @throws IOException for any IO Operation that wasn't ignored.
   */
  public static Map<String, ServiceRecord> extractServiceRecords(
      RegistryOperations operations,
      String parentpath,
      Map<String , RegistryPathStatus> stats) throws IOException {
    return extractServiceRecords(operations, parentpath, stats.values());
  }


  /**
   * Extract all service records under a list of stat operations...this
   * non-atomic action skips entries that are too short or simply not matching.
   * <p>
   * @param operations operation support for fetches
   * @param parentpath path of the parent of all the entries
   * @return a possibly empty map of fullpath:record.
   * @throws IOException for any IO Operation that wasn't ignored.
   */
  public static Map<String, ServiceRecord> extractServiceRecords(
      RegistryOperations operations,
      String parentpath) throws IOException {
    return
    extractServiceRecords(operations,
        parentpath,
        statChildren(operations, parentpath).values());
  }



  /**
   * Static instance of service record marshalling
   */
  public static class ServiceRecordMarshal extends JsonSerDeser<ServiceRecord> {
    public ServiceRecordMarshal() {
      super(ServiceRecord.class);
    }
  }
}
