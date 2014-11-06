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

package org.apache.hadoop.registry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.binding.RegistryTypeUtils;
import org.apache.hadoop.registry.client.types.AddressTypes;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ProtocolTypes;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.registry.secure.AbstractSecureRegistryTest;
import org.apache.zookeeper.common.PathUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.registry.client.binding.RegistryTypeUtils.*;

/**
 * This is a set of static methods to aid testing the registry operations.
 * The methods can be imported statically —or the class used as a base
 * class for tests.
 */
public class RegistryTestHelper extends Assert {
  public static final String SC_HADOOP = "org-apache-hadoop";
  public static final String USER = "devteam/";
  public static final String NAME = "hdfs";
  public static final String API_WEBHDFS = "classpath:org.apache.hadoop.namenode.webhdfs";
  public static final String API_HDFS = "classpath:org.apache.hadoop.namenode.dfs";
  public static final String USERPATH = RegistryConstants.PATH_USERS + USER;
  public static final String PARENT_PATH = USERPATH + SC_HADOOP + "/";
  public static final String ENTRY_PATH = PARENT_PATH + NAME;
  public static final String NNIPC = "uuid:423C2B93-C927-4050-AEC6-6540E6646437";
  public static final String IPC2 = "uuid:0663501D-5AD3-4F7E-9419-52F5D6636FCF";
  private static final Logger LOG =
      LoggerFactory.getLogger(RegistryTestHelper.class);
  private static final RegistryUtils.ServiceRecordMarshal recordMarshal =
      new RegistryUtils.ServiceRecordMarshal();
  public static final String HTTP_API = "http://";

  /**
   * Assert the path is valid by ZK rules
   * @param path path to check
   */
  public static void assertValidZKPath(String path) {
    try {
      PathUtils.validatePath(path);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException("Invalid Path " + path + ": " + e, e);
    }
  }

  /**
   * Assert that a string is not empty (null or "")
   * @param message message to raise if the string is empty
   * @param check string to check
   */
  public static void assertNotEmpty(String message, String check) {
    if (StringUtils.isEmpty(check)) {
      fail(message);
    }
  }

  /**
   * Assert that a string is empty (null or "")
   * @param check string to check
   */
  public static void assertNotEmpty(String check) {
    if (StringUtils.isEmpty(check)) {
      fail("Empty string");
    }
  }

  /**
   * Log the details of a login context
   * @param name name to assert that the user is logged in as
   * @param loginContext the login context
   */
  public static void logLoginDetails(String name,
      LoginContext loginContext) {
    assertNotNull("Null login context", loginContext);
    Subject subject = loginContext.getSubject();
    LOG.info("Logged in as {}:\n {}", name, subject);
  }

  /**
   * Set the JVM property to enable Kerberos debugging
   */
  public static void enableKerberosDebugging() {
    System.setProperty(AbstractSecureRegistryTest.SUN_SECURITY_KRB5_DEBUG,
        "true");
  }
  /**
   * Set the JVM property to enable Kerberos debugging
   */
  public static void disableKerberosDebugging() {
    System.setProperty(AbstractSecureRegistryTest.SUN_SECURITY_KRB5_DEBUG,
        "false");
  }

  /**
   * General code to validate bits of a component/service entry built iwth
   * {@link #addSampleEndpoints(ServiceRecord, String)}
   * @param record instance to check
   */
  public static void validateEntry(ServiceRecord record) {
    assertNotNull("null service record", record);
    List<Endpoint> endpoints = record.external;
    assertEquals(2, endpoints.size());

    Endpoint webhdfs = findEndpoint(record, API_WEBHDFS, true, 1, 1);
    assertEquals(API_WEBHDFS, webhdfs.api);
    assertEquals(AddressTypes.ADDRESS_URI, webhdfs.addressType);
    assertEquals(ProtocolTypes.PROTOCOL_REST, webhdfs.protocolType);
    List<Map<String, String>> addressList = webhdfs.addresses;
    Map<String, String> url = addressList.get(0);
    String addr = url.get("uri");
    assertTrue(addr.contains("http"));
    assertTrue(addr.contains(":8020"));

    Endpoint nnipc = findEndpoint(record, NNIPC, false, 1,2);
    assertEquals("wrong protocol in " + nnipc, ProtocolTypes.PROTOCOL_THRIFT,
        nnipc.protocolType);

    Endpoint ipc2 = findEndpoint(record, IPC2, false, 1,2);
    assertNotNull(ipc2);

    Endpoint web = findEndpoint(record, HTTP_API, true, 1, 1);
    assertEquals(1, web.addresses.size());
    assertEquals(1, web.addresses.get(0).size());
  }

  /**
   * Assert that an endpoint matches the criteria
   * @param endpoint endpoint to examine
   * @param addressType expected address type
   * @param protocolType expected protocol type
   * @param api API
   */
  public static void assertMatches(Endpoint endpoint,
      String addressType,
      String protocolType,
      String api) {
    assertNotNull(endpoint);
    assertEquals(addressType, endpoint.addressType);
    assertEquals(protocolType, endpoint.protocolType);
    assertEquals(api, endpoint.api);
  }

  /**
   * Assert the records match.
   * @param source record that was written
   * @param resolved the one that resolved.
   */
  public static void assertMatches(ServiceRecord source, ServiceRecord resolved) {
    assertNotNull("Null source record ", source);
    assertNotNull("Null resolved record ", resolved);
    assertEquals(source.description, resolved.description);

    Map<String, String> srcAttrs = source.attributes();
    Map<String, String> resolvedAttrs = resolved.attributes();
    String sourceAsString = source.toString();
    String resolvedAsString = resolved.toString();
    assertEquals("Wrong count of attrs in \n" + sourceAsString
                 + "\nfrom\n" + resolvedAsString,
        srcAttrs.size(),
        resolvedAttrs.size());
    for (Map.Entry<String, String> entry : srcAttrs.entrySet()) {
      String attr = entry.getKey();
      assertEquals("attribute "+ attr, entry.getValue(), resolved.get(attr));
    }
    assertEquals("wrong external endpoint count",
        source.external.size(), resolved.external.size());
    assertEquals("wrong external endpoint count",
        source.internal.size(), resolved.internal.size());
  }

  /**
   * Find an endpoint in a record or fail,
   * @param record record
   * @param api API
   * @param external external?
   * @param addressElements expected # of address elements?
   * @param addressTupleSize expected size of a type
   * @return the endpoint.
   */
  public static Endpoint findEndpoint(ServiceRecord record,
      String api, boolean external, int addressElements, int addressTupleSize) {
    Endpoint epr = external ? record.getExternalEndpoint(api)
                            : record.getInternalEndpoint(api);
    if (epr != null) {
      assertEquals("wrong # of addresses",
          addressElements, epr.addresses.size());
      assertEquals("wrong # of elements in an address tuple",
          addressTupleSize, epr.addresses.get(0).size());
      return epr;
    }
    List<Endpoint> endpoints = external ? record.external : record.internal;
    StringBuilder builder = new StringBuilder();
    for (Endpoint endpoint : endpoints) {
      builder.append("\"").append(endpoint).append("\" ");
    }
    fail("Did not find " + api + " in endpoints " + builder);
    // never reached; here to keep the compiler happy
    return null;
  }

  /**
   * Log a record
   * @param name record name
   * @param record details
   * @throws IOException only if something bizarre goes wrong marshalling
   * a record.
   */
  public static void logRecord(String name, ServiceRecord record) throws
      IOException {
    LOG.info(" {} = \n{}\n", name, recordMarshal.toJson(record));
  }

  /**
   * Create a service entry with the sample endpoints
   * @param persistence persistence policy
   * @return the record
   * @throws IOException on a failure
   */
  public static ServiceRecord buildExampleServiceEntry(String persistence) throws
      IOException,
      URISyntaxException {
    ServiceRecord record = new ServiceRecord();
    record.set(YarnRegistryAttributes.YARN_ID, "example-0001");
    record.set(YarnRegistryAttributes.YARN_PERSISTENCE, persistence);
    addSampleEndpoints(record, "namenode");
    return record;
  }

  /**
   * Add some endpoints
   * @param entry entry
   */
  public static void addSampleEndpoints(ServiceRecord entry, String hostname)
      throws URISyntaxException {
    assertNotNull(hostname);
    entry.addExternalEndpoint(webEndpoint(HTTP_API,
        new URI("http", hostname + ":80", "/")));
    entry.addExternalEndpoint(
        restEndpoint(API_WEBHDFS,
            new URI("http", hostname + ":8020", "/")));

    Endpoint endpoint = ipcEndpoint(API_HDFS, null);
    endpoint.addresses.add(RegistryTypeUtils.hostnamePortPair(hostname, 8030));
    entry.addInternalEndpoint(endpoint);
    InetSocketAddress localhost = new InetSocketAddress("localhost", 8050);
    entry.addInternalEndpoint(
        inetAddrEndpoint(NNIPC, ProtocolTypes.PROTOCOL_THRIFT, "localhost",
            8050));
    entry.addInternalEndpoint(
        RegistryTypeUtils.ipcEndpoint(
            IPC2, localhost));
  }

  /**
   * Describe the stage in the process with a box around it -so as
   * to highlight it in test logs
   * @param log log to use
   * @param text text
   * @param args logger args
   */
  public static void describe(Logger log, String text, Object...args) {
    log.info("\n=======================================");
    log.info(text, args);
    log.info("=======================================\n");
  }

  /**
   * log out from a context if non-null ... exceptions are caught and logged
   * @param login login context
   * @return null, always
   */
  public static LoginContext logout(LoginContext login) {
    try {
      if (login != null) {
        LOG.debug("Logging out login context {}", login.toString());
        login.logout();
      }
    } catch (LoginException e) {
      LOG.warn("Exception logging out: {}", e, e);
    }
    return null;
  }

  /**
   * Login via a UGI. Requres UGI to have been set up
   * @param user username
   * @param keytab keytab to list
   * @return the UGI
   * @throws IOException
   */
  public static UserGroupInformation loginUGI(String user, File keytab) throws
      IOException {
    LOG.info("Logging in as {} from {}", user, keytab);
    return UserGroupInformation.loginUserFromKeytabAndReturnUGI(user,
        keytab.getAbsolutePath());
  }

  public static ServiceRecord createRecord(String persistence) {
    return createRecord("01", persistence, "description");
  }

  public static ServiceRecord createRecord(String id, String persistence,
      String description) {
    ServiceRecord serviceRecord = new ServiceRecord();
    serviceRecord.set(YarnRegistryAttributes.YARN_ID, id);
    serviceRecord.description = description;
    serviceRecord.set(YarnRegistryAttributes.YARN_PERSISTENCE, persistence);
    return serviceRecord;
  }

  public static ServiceRecord createRecord(String id, String persistence,
      String description, String data) {
    return createRecord(id, persistence, description);
  }
}
