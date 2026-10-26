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
package org.apache.hadoop.hdfs;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.hdfs.qjournal.server.JournalNodeRpcServer;
import org.apache.hadoop.hdfs.server.namenode.NameNodeRpcServer;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.security.authorize.Service;
import org.apache.hadoop.test.TestName;
import org.apache.hadoop.util.Sets;

import org.junit.jupiter.api.BeforeAll;

import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test suite covering HDFSPolicyProvider.  We expect that it contains a
 * security policy definition for every RPC protocol used in HDFS.  The test
 * suite works by scanning an RPC server's class to find the protocol interfaces
 * it implements, and then comparing that to the protocol interfaces covered in
 * HDFSPolicyProvider.  This is a parameterized test repeated for multiple HDFS
 * RPC server classes.
 */
public class TestHDFSPolicyProvider {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestHDFSPolicyProvider.class);

  private static Set<Class<?>> policyProviderProtocols;

  private static final Comparator<Class<?>> CLASS_NAME_COMPARATOR =
      new Comparator<Class<?>>() {
        @Override
        public int compare(Class<?> lhs, Class<?> rhs) {
          return lhs.getName().compareTo(rhs.getName());
        }
      };

  @RegisterExtension
  private TestName methodName = new TestName();

  private Class<?> rpcServerClass;

  @BeforeAll
  public static void initialize() {
    Service[] services = new HDFSPolicyProvider().getServices();
    policyProviderProtocols = new HashSet<>(services.length);
    for (Service service : services) {
      policyProviderProtocols.add(service.getProtocol());
    }
  }

  public void initTestHDFSPolicyProvider(Class<?> pRpcServerClass) {
    this.rpcServerClass = pRpcServerClass;
    initialize();
  }

  public static List<Class<?>[]> data() {
    return Arrays.asList(new Class<?>[][]{
        {NameNodeRpcServer.class},
        {DataNode.class},
        {JournalNodeRpcServer.class}
    });
  }

  @ParameterizedTest(name = "protocolsForServer-{0}")
  @MethodSource("data")
  public void testPolicyProviderForServer(Class<?> pRpcServerClass) {
    initTestHDFSPolicyProvider(pRpcServerClass);
    List<?> ifaces = ClassUtils.getAllInterfaces(rpcServerClass);
    Set<Class<?>> serverProtocols = new HashSet<>(ifaces.size());
    for (Object obj : ifaces) {
      Class<?> iface = (Class<?>)obj;
      if (iface.getSimpleName().endsWith("Protocol")) {
        serverProtocols.add(iface);
      }
    }
    LOG.info("Running test {} for RPC server {}.  Found server protocols {} "
        + "and policy provider protocols {}.", methodName.getMethodName(),
        rpcServerClass.getName(), serverProtocols, policyProviderProtocols);
    assertFalse(serverProtocols.isEmpty(),
        "Expected to find at least one protocol in server.");
    final Set<Class<?>> differenceSet =
        Sets.difference(serverProtocols, policyProviderProtocols);
    assertTrue(differenceSet.isEmpty(),
        String.format("Following protocols for server %s are not defined in "
                + "%s: %s",
            rpcServerClass.getName(), HDFSPolicyProvider.class.getName(),
            Arrays.toString(differenceSet.toArray())));
  }
}
