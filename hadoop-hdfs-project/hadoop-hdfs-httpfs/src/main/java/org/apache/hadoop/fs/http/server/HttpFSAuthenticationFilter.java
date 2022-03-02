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
package org.apache.hadoop.fs.http.server;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.WebHdfsConstants;
import org.apache.hadoop.http.HttpServer2;
import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.util.RandomSignerSecretProvider;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;
import org.apache.hadoop.security.token.delegation.web.DelegationTokenAuthenticationFilter;
import org.apache.hadoop.security.token.delegation.web.KerberosDelegationTokenAuthenticationHandler;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * Subclass of hadoop-auth <code>AuthenticationFilter</code> that obtains its
 * configuration from HttpFSServer's server configuration.
 */
@InterfaceAudience.Private
public class HttpFSAuthenticationFilter
    extends DelegationTokenAuthenticationFilter {

  public static final String CONF_PREFIX = "httpfs.authentication.";
  public static final String HADOOP_HTTP_CONF_PREFIX = "hadoop.http.authentication.";
  static final String[] CONF_PREFIXES = {CONF_PREFIX, HADOOP_HTTP_CONF_PREFIX};

  private static final String SIGNATURE_SECRET_FILE = SIGNATURE_SECRET
      + ".file";

  /**
   * Returns the hadoop-auth configuration from HttpFSServer's configuration.
   * <p>
   * It returns all HttpFSServer's configuration properties prefixed with
   * <code>hadoop.http.authentication</code>. The
   * <code>hadoop.http.authentication</code> prefix is removed from the
   * returned property names.
   *
   * @param configPrefix parameter not used.
   * @param filterConfig parameter not used.
   *
   * @return hadoop-auth configuration read from HttpFSServer's configuration.
   */
  @Override
  protected Properties getConfiguration(String configPrefix,
      FilterConfig filterConfig) throws ServletException{
    Configuration conf = HttpFSServerWebApp.get().getConfig();
    Properties props = HttpServer2.getFilterProperties(conf,
        new ArrayList<>(Arrays.asList(CONF_PREFIXES)));

    String signatureSecretFile = props.getProperty(SIGNATURE_SECRET_FILE, null);
    if (signatureSecretFile == null) {
      throw new RuntimeException("Undefined property: "
          + SIGNATURE_SECRET_FILE);
    }

    if (!isRandomSecret(filterConfig)) {
      try (Reader reader = new InputStreamReader(Files.newInputStream(
          Paths.get(signatureSecretFile)), StandardCharsets.UTF_8)) {
        StringBuilder secret = new StringBuilder();
        int c = reader.read();
        while (c > -1) {
          secret.append((char) c);
          c = reader.read();
        }

        String secretString = secret.toString();
        if (secretString.isEmpty()) {
          throw new RuntimeException(
              "No secret in HttpFs signature secret file: "
                  + signatureSecretFile);
        }

        props.setProperty(AuthenticationFilter.SIGNATURE_SECRET,
            secretString);
      } catch (IOException ex) {
        throw new RuntimeException("Could not read HttpFS signature "
            + "secret file: " + signatureSecretFile);
      }
    }
    setAuthHandlerClass(props);
    String dtkind = WebHdfsConstants.WEBHDFS_TOKEN_KIND.toString();
    if (conf.getBoolean(HttpFSServerWebServer.SSL_ENABLED_KEY, false)) {
      dtkind = WebHdfsConstants.SWEBHDFS_TOKEN_KIND.toString();
    }
    props.setProperty(KerberosDelegationTokenAuthenticationHandler.TOKEN_KIND,
                      dtkind);
    return props;
  }

  protected Configuration getProxyuserConfiguration(FilterConfig filterConfig) {
    Map<String, String> proxyuserConf = HttpFSServerWebApp.get().getConfig().
        getValByRegex("httpfs\\.proxyuser\\.");
    Configuration conf = new Configuration(false);
    for (Map.Entry<String, String> entry : proxyuserConf.entrySet()) {
      conf.set(entry.getKey().substring("httpfs.".length()), entry.getValue());
    }
    return conf;
  }

  private boolean isRandomSecret(FilterConfig filterConfig) {
    SignerSecretProvider secretProvider = (SignerSecretProvider) filterConfig
        .getServletContext().getAttribute(SIGNER_SECRET_PROVIDER_ATTRIBUTE);
    if (secretProvider == null) {
      return false;
    }
    return secretProvider.getClass() == RandomSignerSecretProvider.class;
  }
}
