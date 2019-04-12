/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.ozone;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdds.HddsUtils;
import org.apache.hadoop.hdds.cli.GenericCli;
import org.apache.hadoop.hdds.cli.HddsVersionProvider;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.hdds.protocol.DatanodeDetails;
import org.apache.hadoop.hdds.protocol.SCMSecurityProtocol;
import org.apache.hadoop.hdds.scm.HddsServerUtil;
import org.apache.hadoop.hdds.scm.ScmConfigKeys;
import org.apache.hadoop.hdds.security.x509.SecurityConfig;
import org.apache.hadoop.hdds.security.x509.certificate.client.CertificateClient;
import org.apache.hadoop.hdds.security.x509.certificate.client.DNCertificateClient;
import org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest;
import org.apache.hadoop.hdds.tracing.TracingUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.apache.hadoop.ozone.container.common.helpers.ContainerUtils;
import org.apache.hadoop.ozone.container.common.statemachine.DatanodeStateMachine;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.util.ServicePlugin;
import org.apache.hadoop.util.StringUtils;
import org.bouncycastle.pkcs.PKCS10CertificationRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.KeyPair;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.UUID;

import static org.apache.hadoop.hdds.security.x509.certificate.utils.CertificateCodec.getX509Certificate;
import static org.apache.hadoop.hdds.security.x509.certificates.utils.CertificateSignRequest.getEncodedString;
import static org.apache.hadoop.ozone.OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY;
import static org.apache.hadoop.util.ExitUtil.terminate;

/**
 * Datanode service plugin to start the HDDS container services.
 */

@Command(name = "ozone datanode",
    hidden = true, description = "Start the datanode for ozone",
    versionProvider = HddsVersionProvider.class,
    mixinStandardHelpOptions = true)
public class HddsDatanodeService extends GenericCli implements ServicePlugin {

  private static final Logger LOG = LoggerFactory.getLogger(
      HddsDatanodeService.class);

  private OzoneConfiguration conf;
  private DatanodeDetails datanodeDetails;
  private DatanodeStateMachine datanodeStateMachine;
  private List<ServicePlugin> plugins;
  private CertificateClient dnCertClient;
  private String component;
  private HddsDatanodeHttpServer httpServer;

  /**
   * Default constructor.
   */
  public HddsDatanodeService() {
    this(null);
  }

  /**
   * Constructs {@link HddsDatanodeService} using the provided {@code conf}
   * value.
   *
   * @param conf OzoneConfiguration
   */
  public HddsDatanodeService(Configuration conf) {
    if (conf == null) {
      this.conf = new OzoneConfiguration();
    } else {
      this.conf = new OzoneConfiguration(conf);
    }
  }

  @VisibleForTesting
  public static HddsDatanodeService createHddsDatanodeService(
      String[] args, Configuration conf) {
    return createHddsDatanodeService(args, conf, false);
  }

  /**
   * Create an Datanode instance based on the supplied command-line arguments.
   * <p>
   * This method is intended for unit tests only. It suppresses the
   * startup/shutdown message and skips registering Unix signal handlers.
   *
   * @param args        command line arguments.
   * @param conf        HDDS configuration
   * @param printBanner if true, then log a verbose startup message.
   * @return Datanode instance
   */
  private static HddsDatanodeService createHddsDatanodeService(
      String[] args, Configuration conf, boolean printBanner) {
    if (args.length == 0 && printBanner) {
      StringUtils
          .startupShutdownMessage(HddsDatanodeService.class, args, LOG);

    }
    return new HddsDatanodeService(conf);
  }

  public static void main(String[] args) {
    try {
      Configuration conf = new OzoneConfiguration();
      HddsDatanodeService hddsDatanodeService =
          createHddsDatanodeService(args, conf, true);
      if (hddsDatanodeService != null) {
        hddsDatanodeService.start(null);
        hddsDatanodeService.join();
      }
    } catch (Throwable e) {
      LOG.error("Exception in HddsDatanodeService.", e);
      terminate(1, e);
    }
  }

  public static Logger getLogger() {
    return LOG;
  }

  /**
   * Starts HddsDatanode services.
   *
   * @param service The service instance invoking this method
   */
  @Override
  public void start(Object service) {

    DefaultMetricsSystem.initialize("HddsDatanode");
    OzoneConfiguration.activate();
    if (service instanceof Configurable) {
      conf = new OzoneConfiguration(((Configurable) service).getConf());
    }
    if (HddsUtils.isHddsEnabled(conf)) {
      try {
        String hostname = HddsUtils.getHostName(conf);
        String ip = InetAddress.getByName(hostname).getHostAddress();
        datanodeDetails = initializeDatanodeDetails();
        datanodeDetails.setHostName(hostname);
        datanodeDetails.setIpAddress(ip);
        TracingUtil.initTracing(
            "HddsDatanodeService." + datanodeDetails.getUuidString()
                .substring(0, 8));
        LOG.info("HddsDatanodeService host:{} ip:{}", hostname, ip);
        // Authenticate Hdds Datanode service if security is enabled
        if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
          component = "dn-" + datanodeDetails.getUuidString();

          dnCertClient = new DNCertificateClient(new SecurityConfig(conf),
              datanodeDetails.getCertSerialId());

          if (SecurityUtil.getAuthenticationMethod(conf).equals(
              UserGroupInformation.AuthenticationMethod.KERBEROS)) {
            LOG.info("Ozone security is enabled. Attempting login for Hdds " +
                    "Datanode user. Principal: {},keytab: {}", conf.get(
                DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY),
                conf.get(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY));

            UserGroupInformation.setConfiguration(conf);

            SecurityUtil.login(conf, DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY,
                DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, hostname);
          } else {
            throw new AuthenticationException(SecurityUtil.
                getAuthenticationMethod(conf) + " authentication method not " +
                "supported. Datanode user" + " login " + "failed.");
          }
          LOG.info("Hdds Datanode login successful.");
        }
        if (OzoneSecurityUtil.isSecurityEnabled(conf)) {
          initializeCertificateClient(conf);
        }
        datanodeStateMachine = new DatanodeStateMachine(datanodeDetails, conf,
            dnCertClient);
        try {
          httpServer = new HddsDatanodeHttpServer(conf);
          httpServer.start();
        } catch (Exception ex) {
          LOG.error("HttpServer failed to start.", ex);
        }
        startPlugins();
        // Starting HDDS Daemons
        datanodeStateMachine.startDaemon();
      } catch (IOException e) {
        throw new RuntimeException("Can't start the HDDS datanode plugin", e);
      } catch (AuthenticationException ex) {
        throw new RuntimeException("Fail to authentication when starting" +
            " HDDS datanode plugin", ex);
      }
    }
  }

  /**
   * Initializes secure Datanode.
   * */
  @VisibleForTesting
  public void initializeCertificateClient(OzoneConfiguration config)
      throws IOException {
    LOG.info("Initializing secure Datanode.");

    CertificateClient.InitResponse response = dnCertClient.init();
    LOG.info("Init response: {}", response);
    switch (response) {
    case SUCCESS:
      LOG.info("Initialization successful, case:{}.", response);
      break;
    case GETCERT:
      getSCMSignedCert(config);
      LOG.info("Successfully stored SCM signed certificate, case:{}.",
          response);
      break;
    case FAILURE:
      LOG.error("DN security initialization failed, case:{}.", response);
      throw new RuntimeException("DN security initialization failed.");
    case RECOVER:
      LOG.error("DN security initialization failed, case:{}. OM certificate " +
          "is missing.", response);
      throw new RuntimeException("DN security initialization failed.");
    default:
      LOG.error("DN security initialization failed. Init response: {}",
          response);
      throw new RuntimeException("DN security initialization failed.");
    }
  }

  /**
   * Get SCM signed certificate and store it using certificate client.
   * @param config
   * */
  private void getSCMSignedCert(OzoneConfiguration config) {
    try {
      PKCS10CertificationRequest csr = getCSR(config);
      // TODO: For SCM CA we should fetch certificate from multiple SCMs.
      SCMSecurityProtocol secureScmClient =
          HddsUtils.getScmSecurityClient(config,
              HddsUtils.getScmAddressForSecurityProtocol(config));

      String pemEncodedCert = secureScmClient.getDataNodeCertificate(
          datanodeDetails.getProtoBufMessage(), getEncodedString(csr));
      dnCertClient.storeCertificate(pemEncodedCert, true);
      datanodeDetails.setCertSerialId(getX509Certificate(pemEncodedCert).
          getSerialNumber().toString());
      persistDatanodeDetails(datanodeDetails);
    } catch (IOException | CertificateException e) {
      LOG.error("Error while storing SCM signed certificate.", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Creates CSR for DN.
   * @param config
   * */
  @VisibleForTesting
  public PKCS10CertificationRequest getCSR(Configuration config)
      throws IOException {
    CertificateSignRequest.Builder builder = dnCertClient.getCSRBuilder();
    KeyPair keyPair = new KeyPair(dnCertClient.getPublicKey(),
        dnCertClient.getPrivateKey());

    String hostname = InetAddress.getLocalHost().getCanonicalHostName();
    String subject = UserGroupInformation.getCurrentUser()
        .getShortUserName() + "@" + hostname;

    builder.setCA(false)
        .setKey(keyPair)
        .setConfiguration(config)
        .setSubject(subject);

    LOG.info("Creating csr for DN-> subject:{}", subject);
    return builder.build();
  }

  /**
   * Returns DatanodeDetails or null in case of Error.
   *
   * @return DatanodeDetails
   */
  private DatanodeDetails initializeDatanodeDetails()
      throws IOException {
    String idFilePath = HddsServerUtil.getDatanodeIdFilePath(conf);
    if (idFilePath == null || idFilePath.isEmpty()) {
      LOG.error("A valid file path is needed for config setting {}",
          ScmConfigKeys.OZONE_SCM_DATANODE_ID);
      throw new IllegalArgumentException(ScmConfigKeys.OZONE_SCM_DATANODE_ID +
          " must be defined. See" +
          " https://wiki.apache.org/hadoop/Ozone#Configuration" +
          " for details on configuring Ozone.");
    }

    Preconditions.checkNotNull(idFilePath);
    File idFile = new File(idFilePath);
    if (idFile.exists()) {
      return ContainerUtils.readDatanodeDetailsFrom(idFile);
    } else {
      // There is no datanode.id file, this might be the first time datanode
      // is started.
      String datanodeUuid = UUID.randomUUID().toString();
      return DatanodeDetails.newBuilder().setUuid(datanodeUuid).build();
    }
  }

  /**
   * Persist DatanodeDetails to file system.
   * @param dnDetails
   *
   * @return DatanodeDetails
   */
  private void persistDatanodeDetails(DatanodeDetails dnDetails)
      throws IOException {
    String idFilePath = HddsServerUtil.getDatanodeIdFilePath(conf);
    if (idFilePath == null || idFilePath.isEmpty()) {
      LOG.error("A valid file path is needed for config setting {}",
          ScmConfigKeys.OZONE_SCM_DATANODE_ID);
      throw new IllegalArgumentException(ScmConfigKeys.OZONE_SCM_DATANODE_ID +
          " must be defined. See" +
          " https://wiki.apache.org/hadoop/Ozone#Configuration" +
          " for details on configuring Ozone.");
    }

    Preconditions.checkNotNull(idFilePath);
    File idFile = new File(idFilePath);
    ContainerUtils.writeDatanodeDetailsTo(dnDetails, idFile);
  }

  /**
   * Starts all the service plugins which are configured using
   * OzoneConfigKeys.HDDS_DATANODE_PLUGINS_KEY.
   */
  private void startPlugins() {
    try {
      plugins = conf.getInstances(HDDS_DATANODE_PLUGINS_KEY,
          ServicePlugin.class);
    } catch (RuntimeException e) {
      String pluginsValue = conf.get(HDDS_DATANODE_PLUGINS_KEY);
      LOG.error("Unable to load HDDS DataNode plugins. " +
              "Specified list of plugins: {}",
          pluginsValue, e);
      throw e;
    }
    for (ServicePlugin plugin : plugins) {
      try {
        plugin.start(this);
        LOG.info("Started plug-in {}", plugin);
      } catch (Throwable t) {
        LOG.warn("ServicePlugin {} could not be started", plugin, t);
      }
    }
  }

  /**
   * Returns the OzoneConfiguration used by this HddsDatanodeService.
   *
   * @return OzoneConfiguration
   */
  public OzoneConfiguration getConf() {
    return conf;
  }

  /**
   * Return DatanodeDetails if set, return null otherwise.
   *
   * @return DatanodeDetails
   */
  @VisibleForTesting
  public DatanodeDetails getDatanodeDetails() {
    return datanodeDetails;
  }

  @VisibleForTesting
  public DatanodeStateMachine getDatanodeStateMachine() {
    return datanodeStateMachine;
  }

  public void join() {
    try {
      datanodeStateMachine.join();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOG.info("Interrupted during StorageContainerManager join.");
    }
  }

  @Override
  public void stop() {
    if (plugins != null) {
      for (ServicePlugin plugin : plugins) {
        try {
          plugin.stop();
          LOG.info("Stopped plug-in {}", plugin);
        } catch (Throwable t) {
          LOG.warn("ServicePlugin {} could not be stopped", plugin, t);
        }
      }
    }
    if (datanodeStateMachine != null) {
      datanodeStateMachine.stopDaemon();
    }
    if (httpServer != null) {
      try {
        httpServer.stop();
      } catch (Exception e) {
        LOG.error("Stopping HttpServer is failed.", e);
      }
    }

  }

  @Override
  public void close() {
    if (plugins != null) {
      for (ServicePlugin plugin : plugins) {
        try {
          plugin.close();
        } catch (Throwable t) {
          LOG.warn("ServicePlugin {} could not be closed", plugin, t);
        }
      }
    }
  }

  @VisibleForTesting
  public String getComponent() {
    return component;
  }

  public CertificateClient getCertificateClient() {
    return dnCertClient;
  }

  @VisibleForTesting
  public void setCertificateClient(CertificateClient client) {
    dnCertClient = client;
  }
}
