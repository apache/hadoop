/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_ADMINISTRATORS;
import static org.apache.hadoop.ozone.OzoneConfigKeys.OZONE_SECURITY_ENABLED_KEY;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.scm.ScmInfo;
import org.apache.hadoop.hdds.scm.server.SCMStorage;
import org.apache.hadoop.hdds.scm.server.StorageContainerManager;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.KerberosAuthException;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test class to for security enabled Ozone cluster.
 */
@InterfaceAudience.Private
public final class TestSecureOzoneCluster {

  private Logger LOGGER = LoggerFactory
      .getLogger(TestSecureOzoneCluster.class);

  private MiniKdc miniKdc;
  private OzoneConfiguration conf;
  private File workDir;
  private static Properties securityProperties;
  private File scmKeytab;
  private File spnegoKeytab;
  private String curUser;

  @Before
  public void init() {
    try {
      conf = new OzoneConfiguration();
      startMiniKdc();
      setSecureConfig(conf);
      createCredentialsInKDC(conf, miniKdc);
    } catch (IOException e) {
      LOGGER.error("Failed to initialize TestSecureOzoneCluster", e);
    } catch (Exception e) {
      LOGGER.error("Failed to initialize TestSecureOzoneCluster", e);
    }
  }

  private void createCredentialsInKDC(Configuration conf, MiniKdc miniKdc)
      throws Exception {
    createPrincipal(scmKeytab,
        conf.get(ScmConfigKeys.OZONE_SCM_KERBEROS_PRINCIPAL_KEY));
    createPrincipal(spnegoKeytab,
        conf.get(ScmConfigKeys.SCM_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY));
  }

  private void createPrincipal(File keytab, String... principal)
      throws Exception {
    miniKdc.createPrincipal(keytab, principal);
  }

  private void startMiniKdc() throws Exception {
    workDir = GenericTestUtils
        .getTestDir(TestSecureOzoneCluster.class.getSimpleName());
    securityProperties = MiniKdc.createConf();
    miniKdc = new MiniKdc(securityProperties, workDir);
    miniKdc.start();
  }

  private void setSecureConfig(Configuration conf) throws IOException {
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    String host = KerberosUtil.getLocalHostName();
    String realm = miniKdc.getRealm();
    curUser = UserGroupInformation.getCurrentUser()
        .getUserName();
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    conf.set(OZONE_ADMINISTRATORS, curUser);

    conf.set(ScmConfigKeys.OZONE_SCM_KERBEROS_PRINCIPAL_KEY,
        "scm/" + host + "@" + realm);
    conf.set(ScmConfigKeys.SCM_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY,
        "HTTP_SCM/" + host + "@" + realm);

    scmKeytab = new File(workDir, "scm.keytab");
    spnegoKeytab = new File(workDir, "http.keytab");

    conf.set(ScmConfigKeys.OZONE_SCM_KERBEROS_KEYTAB_FILE_KEY,
        scmKeytab.getAbsolutePath());
    conf.set(ScmConfigKeys.SCM_WEB_AUTHENTICATION_KERBEROS_KEYTAB_FILE_KEY,
        spnegoKeytab.getAbsolutePath());

  }

  @Test
  public void testSecureScmStartupSuccess() throws Exception {
    final String path = GenericTestUtils
        .getTempPath(UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, true);
    SCMStorage scmStore = new SCMStorage(conf);
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    scmStore.setClusterId(clusterId);
    scmStore.setScmId(scmId);
    // writes the version file properties
    scmStore.initialize();
    StorageContainerManager scm = StorageContainerManager.createSCM(null, conf);
    //Reads the SCM Info from SCM instance
    ScmInfo scmInfo = scm.getClientProtocolServer().getScmInfo();
    Assert.assertEquals(clusterId, scmInfo.getClusterId());
    Assert.assertEquals(scmId, scmInfo.getScmId());
  }

  @Test
  public void testSecureScmStartupFailure() throws Exception {
    final String path = GenericTestUtils
        .getTempPath(UUID.randomUUID().toString());
    Path scmPath = Paths.get(path, "scm-meta");

    OzoneConfiguration conf = new OzoneConfiguration();
    conf.setBoolean(OZONE_SECURITY_ENABLED_KEY, true);
    conf.set(OzoneConfigKeys.OZONE_METADATA_DIRS, scmPath.toString());
    conf.setBoolean(OzoneConfigKeys.OZONE_ENABLED, true);
    conf.set(ScmConfigKeys.OZONE_SCM_KERBEROS_PRINCIPAL_KEY,
        "scm@" + miniKdc.getRealm());
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");

    SCMStorage scmStore = new SCMStorage(conf);
    String clusterId = UUID.randomUUID().toString();
    String scmId = UUID.randomUUID().toString();
    scmStore.setClusterId(clusterId);
    scmStore.setScmId(scmId);
    // writes the version file properties
    scmStore.initialize();
    LambdaTestUtils.intercept(IOException.class,
        "Running in secure mode, but config doesn't have a keytab",
        () -> {
          StorageContainerManager.createSCM(null, conf);
        });

    conf.set(ScmConfigKeys.OZONE_SCM_KERBEROS_PRINCIPAL_KEY,
        "scm/_HOST@EXAMPLE.com");
    conf.set(ScmConfigKeys.OZONE_SCM_KERBEROS_KEYTAB_FILE_KEY,
        "/etc/security/keytabs/scm.keytab");

    LambdaTestUtils.intercept(KerberosAuthException.class, "failure "
            + "to login: for principal:",
        () -> {
          StorageContainerManager.createSCM(null, conf);
        });
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "OAuth2");

    LambdaTestUtils.intercept(IllegalArgumentException.class, "Invalid"
            + " attribute value for hadoop.security.authentication of OAuth2",
        () -> {
          StorageContainerManager.createSCM(null, conf);
        });

    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "KERBEROS_SSL");
    LambdaTestUtils.intercept(AuthenticationException.class,
        "KERBEROS_SSL authentication method not support.",
        () -> {
          StorageContainerManager.createSCM(null, conf);
        });

  }

}
