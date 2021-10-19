/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License. See accompanying LICENSE file.
 */

package org.apache.hadoop.fs.contract.router;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_HTTP_POLICY_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY;
import static org.apache.hadoop.hdfs.client.HdfsClientConfigKeys.DFS_DATA_TRANSFER_PROTECTION_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_KERBEROS_PRINCIPAL_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_KEYTAB_FILE_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_RPC_BIND_HOST_KEY;
import static org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys.DFS_ROUTER_DELEGATION_TOKEN_DRIVER_CLASS;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.server.federation.router.RBFConfigKeys;
import org.apache.hadoop.hdfs.server.federation.store.driver.StateStoreDriver;
import org.apache.hadoop.hdfs.server.federation.store.driver.impl.StateStoreFileImpl;
import org.apache.hadoop.hdfs.server.federation.security.MockDelegationTokenSecretManager;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.ssl.KeyStoreTestUtil;
import org.apache.hadoop.test.GenericTestUtils;

/**
 * Test utility to provide a standard routine to initialize the configuration
 * for secure RBF HDFS cluster.
 */
public final class SecurityConfUtil {

  // SSL keystore
  private static String keystoresDir;
  private static String sslConfDir;

  // State string for mini dfs
  private static final String SPNEGO_USER_NAME = "HTTP";
  private static final String ROUTER_USER_NAME = "router";
  private static final String PREFIX = "hadoop.http.authentication.";

  private static MiniKdc kdc;
  private static File baseDir;

  private static String spnegoPrincipal;
  private static String routerPrincipal;

  private SecurityConfUtil() {
    // Utility Class
  }

  public static String getRouterUserName() {
    return ROUTER_USER_NAME;
  }

  public static Configuration initSecurity() throws Exception {
    // delete old test dir
    baseDir = GenericTestUtils.getTestDir(
        SecurityConfUtil.class.getSimpleName());
    FileUtil.fullyDelete(baseDir);
    assertTrue(baseDir.mkdirs());

    // start a mini kdc with default conf
    Properties kdcConf = MiniKdc.createConf();
    kdc = new MiniKdc(kdcConf, baseDir);
    kdc.start();

    Configuration conf = new HdfsConfiguration();
    SecurityUtil.setAuthenticationMethod(
        UserGroupInformation.AuthenticationMethod.KERBEROS, conf);

    UserGroupInformation.setConfiguration(conf);
    assertTrue("Expected configuration to enable security",
        UserGroupInformation.isSecurityEnabled());

    // Setup the keytab
    File keytabFile = new File(baseDir, "test.keytab");
    String keytab = keytabFile.getAbsolutePath();

    // Windows will not reverse name lookup "127.0.0.1" to "localhost".
    String krbInstance = Path.WINDOWS ? "127.0.0.1" : "localhost";

    kdc.createPrincipal(keytabFile,
        SPNEGO_USER_NAME + "/" + krbInstance,
        ROUTER_USER_NAME + "/" + krbInstance);

    routerPrincipal =
        ROUTER_USER_NAME + "/" + krbInstance + "@" + kdc.getRealm();
    spnegoPrincipal =
        SPNEGO_USER_NAME + "/" + krbInstance + "@" + kdc.getRealm();

    // Setup principles and keytabs for dfs
    conf.set(DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, routerPrincipal);
    conf.set(DFS_NAMENODE_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, routerPrincipal);
    conf.set(DFS_DATANODE_KEYTAB_FILE_KEY, keytab);
    conf.set(PREFIX + "type", "kerberos");
    conf.set(PREFIX + "kerberos.principal", spnegoPrincipal);
    conf.set(PREFIX + "kerberos.keytab", keytab);

    conf.set(DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    conf.setBoolean(DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    conf.set(DFS_DATA_TRANSFER_PROTECTION_KEY, "authentication");
    conf.set(DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());

    // Setup SSL configuration
    keystoresDir = baseDir.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(
        SecurityConfUtil.class);
    KeyStoreTestUtil.setupSSLConfig(
        keystoresDir, sslConfDir, conf, false);
    conf.set(DFS_CLIENT_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getClientSSLConfigFileName());
    conf.set(DFS_SERVER_HTTPS_KEYSTORE_RESOURCE_KEY,
        KeyStoreTestUtil.getServerSSLConfigFileName());

    // Setup principals and keytabs for router
    conf.set(DFS_ROUTER_KEYTAB_FILE_KEY, keytab);
    conf.set(DFS_ROUTER_KERBEROS_PRINCIPAL_KEY, routerPrincipal);
    conf.set(DFS_ROUTER_KERBEROS_INTERNAL_SPNEGO_PRINCIPAL_KEY,
        spnegoPrincipal);

    // Setup basic state store
    conf.setClass(RBFConfigKeys.FEDERATION_STORE_DRIVER_CLASS,
        StateStoreFileImpl.class, StateStoreDriver.class);

    // We need to specify the host to prevent 0.0.0.0 as the host address
    conf.set(DFS_ROUTER_RPC_BIND_HOST_KEY, "localhost");
    conf.set(DFS_ROUTER_DELEGATION_TOKEN_DRIVER_CLASS,
        MockDelegationTokenSecretManager.class.getName());

    return conf;
  }

  public static void destroy() throws Exception {
    if (kdc != null) {
      kdc.stop();
      FileUtil.fullyDelete(baseDir);
      KeyStoreTestUtil.cleanupSSLConfig(keystoresDir, sslConfDir);
    }
  }
}
