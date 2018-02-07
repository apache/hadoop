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
package org.apache.hadoop.yarn.util;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.security.DockerCredentialTokenIdentifier;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Iterator;

/**
 * Commonly needed actions for handling the Docker client configurations.
 *
 * Credentials that are used to access private Docker registries are supplied.
 * Actions include:
 * <ul>
 *   <li>Read the Docker client configuration json file from a
 *   {@link FileSystem}.</li>
 *   <li>Extract the authentication information from the configuration into
 *   {@link Token} and {@link Credentials} objects.</li>
 *   <li>Tokens are commonly shipped via the
 *   {@link org.apache.hadoop.yarn.api.records.ContainerLaunchContext} as a
 *   {@link ByteBuffer}, extract the {@link Credentials}.</li>
 *   <li>Write the Docker client configuration json back to the local filesystem
 *   to be used by the Docker command line.</li>
 * </ul>
 */
public final class DockerClientConfigHandler {
  private static final org.slf4j.Logger LOG =
      LoggerFactory.getLogger(DockerClientConfigHandler.class);

  private static final String CONFIG_AUTHS_KEY = "auths";
  private static final String CONFIG_AUTH_KEY = "auth";

  private DockerClientConfigHandler() { }

  /**
   * Read the Docker client configuration and extract the auth tokens into
   * Credentials.
   *
   * @param configFile the Path to the Docker client configuration.
   * @param conf the Configuration object, needed by the FileSystem.
   * @param applicationId the application ID to associate the Credentials with.
   * @return the populated Credential object with the Docker Tokens.
   * @throws IOException if the file can not be read.
   */
  public static Credentials readCredentialsFromConfigFile(Path configFile,
      Configuration conf, String applicationId) throws IOException {
    // Read the config file
    String contents = null;
    configFile = new Path(configFile.toUri());
    FileSystem fs = configFile.getFileSystem(conf);
    if (fs != null) {
      FSDataInputStream fileHandle = fs.open(configFile);
      if (fileHandle != null) {
        contents = IOUtils.toString(fileHandle);
      }
    }
    if (contents == null) {
      throw new IOException("Failed to read Docker client configuration: "
          + configFile);
    }

    // Parse the JSON and create the Tokens/Credentials.
    ObjectMapper mapper = new ObjectMapper();
    JsonFactory factory = mapper.getJsonFactory();
    JsonParser parser = factory.createJsonParser(contents);
    JsonNode rootNode = mapper.readTree(parser);

    Credentials credentials = new Credentials();
    if (rootNode.has(CONFIG_AUTHS_KEY)) {
      Iterator<String> iter = rootNode.get(CONFIG_AUTHS_KEY).getFieldNames();
      for (; iter.hasNext();) {
        String registryUrl = iter.next();
        String registryCred = rootNode.get(CONFIG_AUTHS_KEY)
            .get(registryUrl)
            .get(CONFIG_AUTH_KEY)
            .asText();
        TokenIdentifier tokenId =
            new DockerCredentialTokenIdentifier(registryUrl, applicationId);
        Token<DockerCredentialTokenIdentifier> token =
            new Token<>(tokenId.getBytes(),
                registryCred.getBytes(Charset.forName("UTF-8")),
                tokenId.getKind(), new Text(registryUrl));
        credentials.addToken(
            new Text(registryUrl + "-" + applicationId), token);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Added token: " + token.toString());
        }
      }
    }
    return credentials;
  }

  /**
   * Convert the Token ByteBuffer to the appropriate Credentials object.
   *
   * @param tokens the Tokens from the ContainerLaunchContext.
   * @return the Credentials object populated from the Tokens.
   */
  public static Credentials getCredentialsFromTokensByteBuffer(
      ByteBuffer tokens) throws IOException {
    Credentials credentials = new Credentials();
    DataInputByteBuffer dibb = new DataInputByteBuffer();
    tokens.rewind();
    dibb.reset(tokens);
    credentials.readTokenStorageStream(dibb);
    tokens.rewind();
    if (LOG.isDebugEnabled()) {
      for (Token token : credentials.getAllTokens()) {
        LOG.debug("Added token: " + token.toString());
      }
    }
    return credentials;
  }

  /**
   * Extract the Docker related tokens from the Credentials and write the Docker
   * client configuration to the supplied File.
   *
   * @param outConfigFile the File to write the Docker client configuration to.
   * @param credentials the populated Credentials object.
   * @throws IOException if the write fails.
   */
  public static void writeDockerCredentialsToPath(File outConfigFile,
      Credentials credentials) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode rootNode = mapper.createObjectNode();
    ObjectNode registryUrlNode = mapper.createObjectNode();
    if (credentials.numberOfTokens() > 0) {
      for (Token<? extends TokenIdentifier> tk : credentials.getAllTokens()) {
        if (tk.getKind().equals(DockerCredentialTokenIdentifier.KIND)) {
          DockerCredentialTokenIdentifier ti =
              (DockerCredentialTokenIdentifier) tk.decodeIdentifier();
          ObjectNode registryCredNode = mapper.createObjectNode();
          registryUrlNode.put(ti.getRegistryUrl(), registryCredNode);
          registryCredNode.put(CONFIG_AUTH_KEY,
              new String(tk.getPassword(), Charset.forName("UTF-8")));
          if (LOG.isDebugEnabled()) {
            LOG.debug("Prepared token for write: " + tk.toString());
          }
        }
      }
    }
    rootNode.put(CONFIG_AUTHS_KEY, registryUrlNode);
    String json =
        mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
    FileUtils.writeStringToFile(outConfigFile, json, Charset.defaultCharset());
  }
}