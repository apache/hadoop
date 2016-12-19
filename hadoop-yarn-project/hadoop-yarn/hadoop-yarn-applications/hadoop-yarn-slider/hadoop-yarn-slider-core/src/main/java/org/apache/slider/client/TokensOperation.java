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

package org.apache.slider.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.slider.common.params.ActionTokensArgs;
import org.apache.slider.core.exceptions.BadClusterStateException;
import org.apache.slider.core.exceptions.NotFoundException;
import static org.apache.slider.core.launch.CredentialUtils.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

public class TokensOperation {

  private static final Logger log = LoggerFactory.getLogger(TokensOperation.class);
  public static final String E_INSECURE
      = "Cluster is not secure -tokens cannot be acquired";
  public static final String E_MISSING_SOURCE_FILE = "Missing source file: ";
  public static final String E_NO_KEYTAB = "No keytab: ";

  public int actionTokens(ActionTokensArgs args, FileSystem fs,
      Configuration conf,
      YarnClientImpl yarnClient)
      throws IOException, YarnException {
    Credentials credentials;
    String footnote = "";
    UserGroupInformation user = UserGroupInformation.getCurrentUser();
    boolean isSecure = UserGroupInformation.isSecurityEnabled();
    if (args.keytab != null) {
      File keytab = args.keytab;
      if (!keytab.isFile()) {
        throw new NotFoundException(E_NO_KEYTAB + keytab.getAbsolutePath());
      }
      String principal = args.principal;
      log.info("Logging in as {} from keytab {}", principal, keytab);
      user = UserGroupInformation.loginUserFromKeytabAndReturnUGI(
          principal, keytab.getCanonicalPath());
    }
    Credentials userCredentials = user.getCredentials();
    File output = args.output;
    if (output != null) {
      if (!isSecure) {
        throw new BadClusterStateException(E_INSECURE);
      }
      credentials = new Credentials(userCredentials);
      // filesystem
      addRMRenewableFSDelegationTokens(conf, fs, credentials);
      addRMDelegationToken(yarnClient, credentials);
      if (maybeAddTimelineToken(conf, credentials) != null) {
        log.debug("Added timeline token");
      }
      saveTokens(output, credentials);
      String filename = output.getCanonicalPath();
      footnote = String.format(
          "%d tokens saved to %s%n" + "To use these in the environment:%n"
              + "export %s=%s", credentials.numberOfTokens(), filename,
          UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION, filename);
    } else if (args.source != null) {
      File source = args.source;
      log.info("Reading credentials from file {}", source);
      if (!source.isFile()) {
        throw new NotFoundException( E_MISSING_SOURCE_FILE + source.getAbsolutePath());
      }
      credentials = Credentials.readTokenStorageFile(args.source, conf);
    } else {
      StringBuffer origin = new StringBuffer();
      File file = locateEnvCredentials(System.getenv(), conf,
          origin);
      if (file != null) {
        log.info("Credential Source {}", origin);
      } else {
        log.info("Credential source: logged in user");
      }
      credentials = userCredentials;
    }
    // list the tokens
    log.info("\n{}", dumpTokens(credentials, "\n"));
    if (!footnote.isEmpty()) {
      log.info(footnote);
    }
    return 0;
  }

}
