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

package org.apache.hadoop.runc.docker;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.runc.docker.auth.BearerAuthSchemeProvider;
import org.apache.hadoop.runc.docker.auth.BearerCredentialsProvider;
import org.apache.hadoop.runc.docker.auth.BearerScheme;
import org.apache.hadoop.runc.docker.model.ManifestListV2;
import org.apache.hadoop.runc.docker.model.ManifestRefV2;
import org.apache.hadoop.runc.docker.model.ManifestV2;
import org.apache.hadoop.runc.docker.model.PlatformV2;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.auth.AuthSchemeProvider;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.config.SocketConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.sasl.AuthenticationException;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class DockerClient implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(DockerClient.class);

  private static final String DOCKER_DISTRIBUTION_API_VERSION_VALUE =
      "registry/2.0";
  private static final String DOCKER_DISTRIBUTION_API_VERSION_HEADER =
      "docker-distribution-api-version";

  private static final String DOCKER_MANIFEST_V2_CT =
      "application/vnd.docker.distribution.manifest.v2+json";
  private static final String DOCKER_MANIFEST_LIST_V2_CT =
      "application/vnd.docker.distribution.manifest.list.v2+json";

  private static final String MANIFEST_ACCEPT =
      DOCKER_MANIFEST_LIST_V2_CT + ", " +
          DOCKER_MANIFEST_V2_CT;

  private final CloseableHttpClient authClient;
  private final CloseableHttpClient client;
  private final BearerCredentialsProvider credsProvider;
  private ManifestChooser manifestChooser
      = new DefaultManifestChooser();

  public DockerClient() {
    credsProvider = new BearerCredentialsProvider();
    authClient = createHttpClient(null, null);
    client = createHttpClient(credsProvider, authClient);
  }

  public void setManifestChooser(ManifestChooser chooser) {
    this.manifestChooser = chooser;
  }

  public ManifestChooser getManifestChooser() {
    return manifestChooser;
  }

  static CloseableHttpClient createHttpClient(
      CredentialsProvider credsProvider,
      CloseableHttpClient authClient) {

    ConnectionConfig connConfig = ConnectionConfig.custom()
        .setCharset(StandardCharsets.UTF_8)
        .build();

    SocketConfig socketConfig = SocketConfig.custom()
        .setSoTimeout(60_000)
        .build();

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(30_000)
        .setSocketTimeout(120_000)
        .setRedirectsEnabled(true)
        .setRelativeRedirectsAllowed(true)
        .setAuthenticationEnabled(true)
        .setConnectionRequestTimeout(30_000)
        .setTargetPreferredAuthSchemes(Arrays.asList("Bearer"))
        .build();

    Registry<AuthSchemeProvider> authSchemeRegistry = authClient == null ? null
        : RegistryBuilder.<AuthSchemeProvider>create()
        .register(BearerScheme.SCHEME, new BearerAuthSchemeProvider(authClient))
        .build();

    List<Header> headers = new ArrayList<>();
    headers.add(new BasicHeader(
        DOCKER_DISTRIBUTION_API_VERSION_HEADER,
        DOCKER_DISTRIBUTION_API_VERSION_VALUE));

    return HttpClients.custom()
        .setDefaultAuthSchemeRegistry(authSchemeRegistry)
        .setDefaultCredentialsProvider(credsProvider)
        .setDefaultConnectionConfig(connConfig)
        .setDefaultSocketConfig(socketConfig)
        .setDefaultRequestConfig(requestConfig)
        .setDefaultHeaders(headers)
        .build();
  }

  public DockerContext createContext(String baseUrl)
      throws IOException, URISyntaxException {

    DockerContext context = new DockerContext(new URL(baseUrl));

    HttpGet get = new HttpGet(context.getBaseUrl().toURI());
    try (CloseableHttpResponse response = client.execute(get)) {
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new AuthenticationException(
            String.format("Unable to authenticate to %s: %s", baseUrl,
                response.getStatusLine()));
      }
    }

    return context;
  }

  public ManifestListV2 listManifests(
      DockerContext context,
      String name,
      String reference)
      throws IOException, URISyntaxException, DockerException {

    URL baseUrl = context.getBaseUrl();

    URL url = new URL(baseUrl, String
        .format("%s/manifests/%s", encodeName(name), encodeName(reference)));

    HttpGet get = new HttpGet(url.toURI());
    get.setHeader(HttpHeaders.ACCEPT, MANIFEST_ACCEPT);

    try (CloseableHttpResponse response = client.execute(get)) {
      if (response.getStatusLine().getStatusCode() != 200) {
        if (response.getStatusLine().getStatusCode() == 404) {
          throw new DockerException(
              String.format("Image not found: %s:%s", name, reference));
        }
        throw new DockerException(
            String.format("Unexpected response [%d %s] from %s",
                response.getStatusLine().getStatusCode(),
                response.getStatusLine().getReasonPhrase(),
                url));
      }

      Header contentType = response.getFirstHeader("content-type");
      if (contentType == null) {
        throw new DockerException(
            String.format("No content type received from %s", url));
      }

      String ct = contentType.getValue();
      LOG.debug("Got manifest content type: {}", ct);

      if (ManifestListV2.matches(ct)) {
        HttpEntity entity = response.getEntity();
        try {
          try (InputStream in = entity.getContent()) {
            ObjectMapper mapper = new ObjectMapper();
            return mapper
                .readerFor(ManifestListV2.class)
                .readValue(in);
          }
        } finally {
          EntityUtils.consumeQuietly(entity);
        }
      } else if (ManifestV2.matches(ct)) {
        Header digest = response.getFirstHeader("docker-content-digest");
        if (digest == null) {
          throw new DockerException("Unable to determine digest for manifest");
        }

        // synthesize a manifest list
        ManifestListV2 manifests = new ManifestListV2();
        manifests.setMediaType(ManifestListV2.CONTENT_TYPE);
        manifests.setSchemaVersion(2);

        ManifestRefV2 mref = new ManifestRefV2();
        mref.setDigest(digest.getValue().trim());
        mref.setMediaType(ManifestRefV2.CONTENT_TYPE);
        mref.setSize(-1);
        mref.setPlatform(new PlatformV2());
        manifests.getManifests().add(mref);

        return manifests;
      } else {
        throw new DockerException(String.format(
            "Unknown content-type %s received from %s", ct, url));
      }
    }
  }

  public byte[] readManifest(
      DockerContext context,
      String name,
      String blob)
      throws IOException, URISyntaxException, DockerException {

    URL baseUrl = context.getBaseUrl();

    URL url = new URL(baseUrl,
        String.format("%s/manifests/%s", encodeName(name), blob));

    LOG.info("Fetching manifest from {}", url);

    HttpGet get = new HttpGet(url.toURI());
    get.setHeader(HttpHeaders.ACCEPT, MANIFEST_ACCEPT);

    try (CloseableHttpResponse response = client.execute(get)) {
      if (response.getStatusLine().getStatusCode() != 200) {
        if (response.getStatusLine().getStatusCode() == 404) {
          throw new DockerException(
              String.format("Manifest not found: %s:%s", name, blob));
        }
        throw new DockerException(
            String.format("Unexpected response [%d %s] from %s",
                response.getStatusLine().getStatusCode(),
                response.getStatusLine().getReasonPhrase(),
                url));
      }

      Header contentType = response.getFirstHeader("content-type");
      if (contentType == null) {
        throw new DockerException(
            String.format("No content type received from %s", url));
      }

      String ct = contentType.getValue();
      LOG.debug("Got manifest content type: {}", ct);

      if (ManifestV2.matches(ct)) {
        HttpEntity entity = response.getEntity();
        try {
          try (InputStream in = entity.getContent()) {
            try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
              IOUtils.copyBytes(in, out, 4096);
              return out.toByteArray();
            }
          }
        } finally {
          EntityUtils.consumeQuietly(entity);
        }
      } else {
        throw new DockerException(String.format(
            "Unknown content-type %s received from %s", ct, url));
      }
    }
  }

  public ManifestV2 parseManifest(byte[] data) throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper
        .readerFor(ManifestV2.class)
        .readValue(data);
  }

  public byte[] readConfig(
      DockerContext context,
      String name,
      String blob)
      throws IOException, URISyntaxException, DockerException {

    URL baseUrl = context.getBaseUrl();

    URL url = new URL(baseUrl,
        String.format("%s/blobs/%s", encodeName(name), blob));

    LOG.info("Fetching config from {}", url);

    HttpGet get = new HttpGet(url.toURI());

    try (CloseableHttpResponse response = client.execute(get)) {
      if (response.getStatusLine().getStatusCode() != 200) {
        if (response.getStatusLine().getStatusCode() == 404) {
          throw new DockerException(
              String.format("Config not found: %s:%s", name, blob));
        }
        throw new DockerException(
            String.format("Unexpected response [%d %s] from %s",
                response.getStatusLine().getStatusCode(),
                response.getStatusLine().getReasonPhrase(),
                url));
      }

      Header contentType = response.getFirstHeader("content-type");
      if (contentType == null) {
        throw new DockerException(
            String.format("No content type received from %s", url));
      }

      String ct = contentType.getValue();
      LOG.debug("Got config content type: {}", ct);

      HttpEntity entity = response.getEntity();
      try {
        try (InputStream in = entity.getContent()) {
          try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(in, out, 4096);
            return out.toByteArray();
          }
        }
      } finally {
        EntityUtils.consumeQuietly(entity);
      }
    }
  }

  public InputStream download(
      DockerContext context, String name, String blob)
      throws IOException, URISyntaxException, DockerException {

    URL baseUrl = context.getBaseUrl();

    URL url = new URL(baseUrl,
        String.format("%s/blobs/%s", encodeName(name), blob));

    HttpGet get = new HttpGet(url.toURI());
    get.setHeader(HttpHeaders.ACCEPT, "application/json");
    CloseableHttpResponse response = client.execute(get);
    if (response.getStatusLine().getStatusCode() != 200) {
      response.close();
      if (response.getStatusLine().getStatusCode() == 404) {
        throw new DockerException(
            String.format("Layer not found: %s [%s]", name, blob));
      }
      throw new DockerException(
          String.format("Unexpected response [%d %s] from %s",
              response.getStatusLine().getStatusCode(),
              response.getStatusLine().getReasonPhrase(),
              url));
    }
    return response.getEntity().getContent();
  }

  @Override
  public void close() throws IOException {
    client.close();
    authClient.close();
  }

  private String encodeName(String name) throws UnsupportedEncodingException {
    String[] parts = name.split("/");
    StringBuilder buf = new StringBuilder();
    for (String part : parts) {
      if (buf.length() > 0) {
        buf.append("/");
      }
      buf.append(URLEncoder.encode(part, "UTF-8"));
    }
    return buf.toString();
  }

}
