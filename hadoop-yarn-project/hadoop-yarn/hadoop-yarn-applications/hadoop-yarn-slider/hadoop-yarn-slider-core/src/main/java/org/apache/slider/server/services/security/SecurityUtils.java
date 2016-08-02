/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.slider.common.SliderKeys;
import org.apache.slider.common.SliderXmlConfKeys;
import org.apache.slider.core.conf.MapOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.nio.file.attribute.PosixFilePermission;
//import java.nio.file.attribute.PosixFilePermissions;


/**
 *
 */
public class SecurityUtils {
  private static final Logger LOG =
      LoggerFactory.getLogger(SecurityUtils.class);

  private static String CA_CONFIG_CONTENTS =  "HOME            = .\n"
                                            + "RANDFILE        = $ENV::HOME/.rnd\n\n"
                                            + "[ ca ]\n"
                                            + "default_ca             = CA_CLIENT\n"
                                            + "[ CA_CLIENT ]\n"
                                            + "dir                    = ${SEC_DIR}/db\n"
                                            + "certs                  = $dir/certs\n"
                                            + "new_certs_dir          = $dir/newcerts\n"
                                            + "\n"
                                            + "database               = $dir/index.txt\n"
                                            + "serial                 = $dir/serial\n"
                                            + "default_days           = 365    \n"
                                            + "\n"
                                            + "default_crl_days       = 7  \n"
                                            + "default_md             = sha256 \n"
                                            + "\n"
                                            + "policy                 = policy_anything \n"
                                            + "\n"
                                            + "[ policy_anything ]\n"
                                            + "countryName            = optional\n"
                                            + "stateOrProvinceName    = optional\n"
                                            + "localityName           = optional\n"
                                            + "organizationName       = optional\n"
                                            + "organizationalUnitName = optional\n"
                                            + "commonName             = optional\n"
                                            + "emailAddress           = optional\n"
                                            + "\n"
                                            + "[req]\n"
                                            + "distinguished_name     = req_distinguished_name\n"
                                            + "\n"
                                            + "[ req_distinguished_name ]\n"
                                            + "\n"
                                            + "[ jdk7_ca ]\n"
                                            + "subjectKeyIdentifier = hash\n"
                                            + "authorityKeyIdentifier = keyid:always,issuer:always\n"
                                            + "basicConstraints = CA:true\n";

  private static final String PASS_TOKEN = "pass:";
  private static String keystorePass;
  private static String securityDir;

  public static void logOpenSslExitCode(String command, int exitCode) {
    if (exitCode == 0) {
      LOG.info(getOpenSslCommandResult(command, exitCode));
    } else {
      LOG.warn(getOpenSslCommandResult(command, exitCode));
    }

  }

  public static String hideOpenSslPassword(String command){
    int start = command.indexOf(PASS_TOKEN);
    while (start >= 0) {
      start += PASS_TOKEN.length();
      CharSequence cs = command.subSequence(start, command.indexOf(" ", start));
      command = command.replace(cs, "****");
      start = command.indexOf(PASS_TOKEN, start + 1);
    }
    return command;
  }

  public static String getOpenSslCommandResult(String command, int exitCode) {
    return new StringBuilder().append("Command ")
        .append(hideOpenSslPassword(command))
        .append(" was finished with exit code: ")
        .append(exitCode).append(" - ")
        .append(getOpenSslExitCodeDescription(exitCode)).toString();
  }

  private static String getOpenSslExitCodeDescription(int exitCode) {
    switch (exitCode) {
      case 0: {
        return "the operation was completed successfully.";
      }
      case 1: {
        return "an error occurred parsing the command options.";
      }
      case 2: {
        return "one of the input files could not be read.";
      }
      case 3: {
        return "an error occurred creating the PKCS#7 file or when reading the MIME message.";
      }
      case 4: {
        return "an error occurred decrypting or verifying the message.";
      }
      case 5: {
        return "the message was verified correctly but an error occurred writing out the signers certificates.";
      }
      default:
        return "unsupported code";
    }
  }

  public static void writeCaConfigFile(String path) throws IOException {
    String contents = CA_CONFIG_CONTENTS.replace("${SEC_DIR}", path);
    FileUtils.writeStringToFile(new File(path, "ca.config"), contents);
  }

  public static String getKeystorePass() {
    return keystorePass;
  }

  public static String getSecurityDir() {
    return securityDir;
  }

  public static void    initializeSecurityParameters(MapOperations configMap) {
    initializeSecurityParameters(configMap, false);
  }

  public static void initializeSecurityParameters(MapOperations configMap,
                                                boolean persistPassword) {
    String keyStoreLocation = configMap.getOption(
        SliderXmlConfKeys.KEY_KEYSTORE_LOCATION, getDefaultKeystoreLocation());
    if (keyStoreLocation == null) {
      LOG.error(SliderXmlConfKeys.KEY_KEYSTORE_LOCATION
          + " is not specified. Unable to initialize security params.");
      return;
    }
    File secDirFile = new File(keyStoreLocation).getParentFile();
    if (!secDirFile.exists()) {
      // create entire required directory structure
      File dbDir = new File(secDirFile, "db");
      File newCertsDir = new File(dbDir, "newcerts");
      newCertsDir.mkdirs();
      RawLocalFileSystem fileSystem = null;
      try {
        fileSystem = new RawLocalFileSystem();
        FsPermission permissions = new FsPermission(FsAction.ALL, FsAction.NONE,
                                                    FsAction.NONE);
        fileSystem.setPermission(new Path(dbDir.getAbsolutePath()),
                                 permissions);
        fileSystem.setPermission(new Path(dbDir.getAbsolutePath()), permissions);
        fileSystem.setPermission(new Path(newCertsDir.getAbsolutePath()),
                                 permissions);
        File indexFile = new File(dbDir, "index.txt");
        indexFile.createNewFile();
        SecurityUtils.writeCaConfigFile(secDirFile.getAbsolutePath().replace('\\', '/'));

      } catch (IOException e) {
        LOG.error("Unable to create SSL configuration directories/files", e);
      } finally {
        if (fileSystem != null) {
          try {
            fileSystem.close();
          } catch (IOException e) {
            LOG.warn("Unable to close fileSystem", e);
          }
        }
      }
      // need to create the password
    }
    keystorePass = getKeystorePassword(secDirFile, persistPassword);
    securityDir = secDirFile.getAbsolutePath();
  }

  private static String getKeystorePassword(File secDirFile,
                                            boolean persistPassword) {
    File passFile = new File(secDirFile, SliderKeys.CRT_PASS_FILE_NAME);
    String password = null;
    if (!passFile.exists()) {
      LOG.info("Generating keystore password");
      password = RandomStringUtils.randomAlphanumeric(
          Integer.valueOf(SliderKeys.PASS_LEN));
      if (persistPassword) {
        try {
          FileUtils.writeStringToFile(passFile, password);
          passFile.setWritable(true);
          passFile.setReadable(true);
        } catch (IOException e) {
          e.printStackTrace();
          throw new RuntimeException(
              "Error creating certificate password file");
        }
      }
    } else {
      LOG.info("Reading password from existing file");
      try {
        password = FileUtils.readFileToString(passFile);
        password = password.replaceAll("\\p{Cntrl}", "");
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    return password;
  }

  private static String getDefaultKeystoreLocation() {
    File workDir = null;
    try {
      workDir =  new File(FileUtils.getTempDirectory().getAbsolutePath()
                          + "/sec" + System.currentTimeMillis());
      if (!workDir.mkdirs()) {
        throw new IOException("Unable to create temporary security directory");
      }
    } catch (IOException e) {
      LOG.warn("Unable to create security directory");
      return null;
    }

    return new StringBuilder().append(workDir.getAbsolutePath())
        .append(File.separator)
        .append(SliderKeys.SECURITY_DIR)
        .append(File.separator)
        .append(SliderKeys.KEYSTORE_FILE_NAME).toString();
  }

}
