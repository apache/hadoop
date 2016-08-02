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
package org.apache.slider.server.services.security;

import com.google.inject.Singleton;
import org.apache.commons.io.FileUtils;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.core.conf.MapOperations;
import org.apache.slider.core.exceptions.SliderException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.text.MessageFormat;

@Singleton
public class CertificateManager {

  private static final Logger LOG =
      LoggerFactory.getLogger(CertificateManager.class);

  private static final String GEN_SRVR_KEY = "openssl genrsa -des3 " +
      "-passout pass:{0} -out {1}" + File.separator + "{2} 4096 ";
  private static final String GEN_SRVR_REQ = "openssl req -passin pass:{0} " +
      "-new -key {1}" + File.separator + "{2} -out {1}" + File.separator +
      "{5} -config {1}" + File.separator + "ca.config " +
      "-subj {6} -batch";
  private static final String SIGN_SRVR_CRT = "openssl ca -create_serial " +
    "-out {1}" + File.separator + "{3} -days 365 -keyfile {1}" + File.separator
    + "{2} -key {0} -selfsign -extensions jdk7_ca -config {1}" + File.separator
    + "ca.config -batch -infiles {1}" + File.separator + "{5}";
  private static final String EXPRT_KSTR = "openssl pkcs12 -export" +
      " -in {2}" + File.separator + "{4} -inkey {2}" + File.separator +
      "{3} -certfile {2}" + File.separator + "{4} -out {2}" + File.separator +
      "{5} -password pass:{1} -passin pass:{0} \n";
  private static final String REVOKE_AGENT_CRT = "openssl ca " +
      "-config {0}" + File.separator + "ca.config -keyfile {0}" +
      File.separator + "{4} -revoke {0}" + File.separator + "{2} -batch " +
      "-passin pass:{3} -cert {0}" + File.separator + "{5}";
  private static final String SIGN_AGENT_CRT = "openssl ca -config " +
      "{0}" + File.separator + "ca.config -in {0}" + File.separator +
      "{1} -out {0}" + File.separator + "{2} -batch -passin pass:{3} " +
      "-keyfile {0}" + File.separator + "{4} -cert {0}" + File.separator + "{5}";
  private static final String GEN_AGENT_KEY="openssl req -new -newkey " +
      "rsa:1024 -nodes -keyout {0}" + File.separator +
      "{2}.key -subj {1} -out {0}" + File.separator + "{2}.csr " +
      "-config {3}" + File.separator + "ca.config ";
  private String passphrase;
  private String applicationName;


  public void initialize(MapOperations compOperations) throws SliderException {
    String hostname = null;
    try {
      hostname = InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException e) {
      hostname = "localhost";
    }
    this.initialize(compOperations, hostname, null, null);
  }

  /**
    * Verify that root certificate exists, generate it otherwise.
    */
  public void initialize(MapOperations compOperations,
                         String hostname, String containerId,
                         String appName) throws SliderException {
    SecurityUtils.initializeSecurityParameters(compOperations);

    LOG.info("Initialization of root certificate");
    boolean certExists = isCertExists();
    LOG.info("Certificate exists:" + certExists);

    this.applicationName = appName;

    if (!certExists) {
      generateAMKeystore(hostname, containerId);
    }

  }

  /**
   * Checks root certificate state.
   * @return "true" if certificate exists
   */
  private boolean isCertExists() {

    String srvrKstrDir = SecurityUtils.getSecurityDir();
    String srvrCrtName = SliderKeys.CRT_FILE_NAME;
    File certFile = new File(srvrKstrDir + File.separator + srvrCrtName);
    LOG.debug("srvrKstrDir = " + srvrKstrDir);
    LOG.debug("srvrCrtName = " + srvrCrtName);
    LOG.debug("certFile = " + certFile.getAbsolutePath());

    return certFile.exists();
  }

  public void setPassphrase(String passphrase) {
    this.passphrase = passphrase;
  }

  class StreamConsumer extends Thread
  {
    InputStream is;
    boolean logOutput;

    StreamConsumer(InputStream is, boolean logOutput)
    {
      this.is = is;
      this.logOutput = logOutput;
    }

    StreamConsumer(InputStream is)
    {
      this(is, false);
    }

    public void run()
    {
      try
      {
        InputStreamReader isr = new InputStreamReader(is,
                                                      Charset.forName("UTF8"));
        BufferedReader br = new BufferedReader(isr);
        String line;
        while ( (line = br.readLine()) != null)
          if (logOutput) {
            LOG.info(line);
          }
      } catch (IOException e)
      {
        LOG.error("Error during processing of process stream", e);
      }
    }
  }


  /**
   * Runs os command
   *
   * @return command execution exit code
   */
  private int runCommand(String command) throws SliderException {
    int exitCode = -1;
    String line = null;
    Process process = null;
    BufferedReader br= null;
    try {
      process = Runtime.getRuntime().exec(command);
      StreamConsumer outputConsumer =
          new StreamConsumer(process.getInputStream(), true);
      StreamConsumer errorConsumer =
          new StreamConsumer(process.getErrorStream(), true);

      outputConsumer.start();
      errorConsumer.start();

      try {
        process.waitFor();
        SecurityUtils.logOpenSslExitCode(command, process.exitValue());
        exitCode = process.exitValue();
        if (exitCode != 0) {
          throw new SliderException(exitCode, "Error running command %s", command);
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (br != null) {
        try {
          br.close();
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }
    }

    return exitCode;//some exception occurred

  }

  public synchronized void generateContainerCertificate(String hostname,
                                                        String identifier) {
    LOG.info("Generation of certificate for {}", hostname);

    String srvrKstrDir = SecurityUtils.getSecurityDir();
    Object[] scriptArgs = {srvrKstrDir, getSubjectDN(hostname, identifier,
        this.applicationName), identifier, SecurityUtils.getSecurityDir()};

    try {
      String command = MessageFormat.format(GEN_AGENT_KEY, scriptArgs);
      runCommand(command);

      signAgentCertificate(identifier);

    } catch (SliderException e) {
      LOG.error("Error generating the agent certificate", e);
    }
  }

  public synchronized SecurityStore generateContainerKeystore(String hostname,
                                                              String requesterId,
                                                              String role,
                                                              String keystorePass)
      throws SliderException {
    LOG.info("Generation of container keystore for container {} on {}",
             requesterId, hostname);

    generateContainerCertificate(hostname, requesterId);

    // come up with correct args to invoke keystore command
    String srvrCrtPass = SecurityUtils.getKeystorePass();
    String srvrKstrDir = SecurityUtils.getSecurityDir();
    String containerCrtName = requesterId + ".crt";
    String containerKeyName = requesterId + ".key";
    String kstrName = getKeystoreFileName(requesterId, role);

    Object[] scriptArgs = {srvrCrtPass, keystorePass, srvrKstrDir,
        containerKeyName, containerCrtName, kstrName};

    String command = MessageFormat.format(EXPRT_KSTR, scriptArgs);
    runCommand(command);

    return new SecurityStore(new File(srvrKstrDir, kstrName),
                             SecurityStore.StoreType.keystore);
  }

  private static String getKeystoreFileName(String containerId,
                                            String role) {
    return String.format("keystore-%s-%s.p12", containerId,
                         role != null ? role : "");
  }

  private void generateAMKeystore(String hostname, String containerId)
      throws SliderException {
    LOG.info("Generation of server certificate");

    String srvrKstrDir = SecurityUtils.getSecurityDir();
    String srvrCrtName = SliderKeys.CRT_FILE_NAME;
    String srvrCsrName = SliderKeys.CSR_FILE_NAME;
    String srvrKeyName = SliderKeys.KEY_FILE_NAME;
    String kstrName = SliderKeys.KEYSTORE_FILE_NAME;
    String srvrCrtPass = SecurityUtils.getKeystorePass();

    Object[] scriptArgs = {srvrCrtPass, srvrKstrDir, srvrKeyName,
        srvrCrtName, kstrName, srvrCsrName, getSubjectDN(hostname, containerId,
        this.applicationName)};

    String command = MessageFormat.format(GEN_SRVR_KEY, scriptArgs);
    runCommand(command);

    command = MessageFormat.format(GEN_SRVR_REQ, scriptArgs);
    runCommand(command);

    command = MessageFormat.format(SIGN_SRVR_CRT, scriptArgs);
    runCommand(command);

    Object[] keystoreArgs = {srvrCrtPass, srvrCrtPass, srvrKstrDir, srvrKeyName,
        srvrCrtName, kstrName, srvrCsrName};
    command = MessageFormat.format(EXPRT_KSTR, keystoreArgs);
    runCommand(command);
  }

  public SecurityStore generateContainerTruststore(String containerId,
                                                   String role,
                                                   String truststorePass)
      throws SliderException {

    String srvrKstrDir = SecurityUtils.getSecurityDir();
    String srvrCrtName = SliderKeys.CRT_FILE_NAME;
    String srvrCsrName = SliderKeys.CSR_FILE_NAME;
    String srvrKeyName = SliderKeys.KEY_FILE_NAME;
    String kstrName = getTruststoreFileName(role, containerId);
    String srvrCrtPass = SecurityUtils.getKeystorePass();

    Object[] scriptArgs = {srvrCrtPass, truststorePass, srvrKstrDir, srvrKeyName,
        srvrCrtName, kstrName, srvrCsrName};

    String command = MessageFormat.format(EXPRT_KSTR, scriptArgs);
    runCommand(command);

    return new SecurityStore(new File(srvrKstrDir, kstrName),
                             SecurityStore.StoreType.truststore);
  }

  private static String getTruststoreFileName(String role, String containerId) {
    return String.format("truststore-%s-%s.p12", containerId,
                         role != null ? role : "");
  }

  /**
   * Returns server certificate content
   * @return string with server certificate content
   */
  public String getServerCert() {
    File certFile = getServerCertficateFilePath();
    String srvrCrtContent = null;
    try {
      srvrCrtContent = FileUtils.readFileToString(certFile);
    } catch (IOException e) {
      LOG.error(e.getMessage());
    }
    return srvrCrtContent;
  }

  public static File getServerCertficateFilePath() {
    return new File(String.format("%s%s%s",
                                  SecurityUtils.getSecurityDir(),
                                  File.separator,
                                  SliderKeys.CRT_FILE_NAME));
  }

  public static File getAgentCertficateFilePath(String containerId) {
    return new File(String.format("%s%s%s.crt",
                                  SecurityUtils.getSecurityDir(),
                                  File.separator,
                                  containerId));
  }

  public static File getContainerKeystoreFilePath(String containerId,
                                                  String role) {
    return new File(SecurityUtils.getSecurityDir(), getKeystoreFileName(
        containerId,
        role
    ));
  }

  public static File getContainerTruststoreFilePath(String role,
                                                    String containerId) {
    return new File(SecurityUtils.getSecurityDir(),
                    getTruststoreFileName(role, containerId));
  }

  public static File getAgentKeyFilePath(String containerId) {
    return new File(String.format("%s%s%s.key",
                                  SecurityUtils.getSecurityDir(),
                                  File.separator,
                                  containerId));
  }

  /**
   * Signs agent certificate
   * Adds agent certificate to server keystore
   * @return string with agent signed certificate content
   */
  public synchronized SignCertResponse signAgentCrt(String agentHostname,
                                                    String agentCrtReqContent,
                                                    String passphraseAgent) {
    SignCertResponse response = new SignCertResponse();
    LOG.info("Signing of agent certificate");
    LOG.info("Verifying passphrase");

    if (!this.passphrase.equals(passphraseAgent.trim())) {
      LOG.warn("Incorrect passphrase from the agent");
      response.setResult(SignCertResponse.ERROR_STATUS);
      response.setMessage("Incorrect passphrase from the agent");
      return response;
    }

    String srvrKstrDir = SecurityUtils.getSecurityDir();
    String srvrCrtPass = SecurityUtils.getKeystorePass();
    String srvrCrtName = SliderKeys.CRT_FILE_NAME;
    String srvrKeyName = SliderKeys.KEY_FILE_NAME;
    String agentCrtReqName = agentHostname + ".csr";
    String agentCrtName = agentHostname + ".crt";

    Object[] scriptArgs = {srvrKstrDir, agentCrtReqName, agentCrtName,
        srvrCrtPass, srvrKeyName, srvrCrtName};

    //Revoke previous agent certificate if exists
    File agentCrtFile = new File(srvrKstrDir + File.separator + agentCrtName);

    String command = null;
    if (agentCrtFile.exists()) {
      LOG.info("Revoking of " + agentHostname + " certificate.");
      command = MessageFormat.format(REVOKE_AGENT_CRT, scriptArgs);
      try {
        runCommand(command);
      } catch (SliderException e) {
        int commandExitCode = e.getExitCode();
        response.setResult(SignCertResponse.ERROR_STATUS);
        response.setMessage(
            SecurityUtils.getOpenSslCommandResult(command, commandExitCode));
        return response;
      }
    }

    File agentCrtReqFile = new File(srvrKstrDir + File.separator +
        agentCrtReqName);
    try {
      FileUtils.writeStringToFile(agentCrtReqFile, agentCrtReqContent);
    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }

    command = MessageFormat.format(SIGN_AGENT_CRT, scriptArgs);

    LOG.debug(SecurityUtils.hideOpenSslPassword(command));
    try {
      runCommand(command);
    } catch (SliderException e) {
      int commandExitCode = e.getExitCode();
      response.setResult(SignCertResponse.ERROR_STATUS);
      response.setMessage(
          SecurityUtils.getOpenSslCommandResult(command, commandExitCode));
      return response;
    }

    String agentCrtContent = "";
    try {
      agentCrtContent = FileUtils.readFileToString(agentCrtFile);
    } catch (IOException e) {
      e.printStackTrace();
      LOG.error("Error reading signed agent certificate");
      response.setResult(SignCertResponse.ERROR_STATUS);
      response.setMessage("Error reading signed agent certificate");
      return response;
    }
    response.setResult(SignCertResponse.OK_STATUS);
    response.setSignedCa(agentCrtContent);
    //LOG.info(ShellCommandUtil.getOpenSslCommandResult(command, commandExitCode));
    return response;
  }

  private String signAgentCertificate (String containerId)
      throws SliderException {
    String srvrKstrDir = SecurityUtils.getSecurityDir();
    String srvrCrtPass = SecurityUtils.getKeystorePass();
    String srvrCrtName = SliderKeys.CRT_FILE_NAME;
    String srvrKeyName = SliderKeys.KEY_FILE_NAME;
    String agentCrtReqName = containerId + ".csr";
    String agentCrtName = containerId + ".crt";

    // server certificate must exist already
    if (!(new File(srvrKstrDir, srvrCrtName).exists())) {
      throw new SliderException("CA certificate not generated");
    }

    Object[] scriptArgs = {srvrKstrDir, agentCrtReqName, agentCrtName,
        srvrCrtPass, srvrKeyName, srvrCrtName};

    //Revoke previous agent certificate if exists
    File agentCrtFile = new File(srvrKstrDir + File.separator + agentCrtName);

    String command;
    if (agentCrtFile.exists()) {
      LOG.info("Revoking of " + containerId + " certificate.");
      command = MessageFormat.format(REVOKE_AGENT_CRT, scriptArgs);
      runCommand(command);
    }

    command = MessageFormat.format(SIGN_AGENT_CRT, scriptArgs);

    LOG.debug(SecurityUtils.hideOpenSslPassword(command));
    runCommand(command);

    return agentCrtName;

  }

  private String getSubjectDN(String hostname, String containerId,
                              String appName) {
    return String.format("/CN=%s%s%s",
                         hostname,
                         containerId != null ? "/OU=" + containerId : "",
                         appName != null ? "/OU=" + appName : "");


  }
}
