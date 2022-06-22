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
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.runc.docker.model.BlobV2;
import org.apache.hadoop.runc.docker.model.ManifestListV2;
import org.apache.hadoop.runc.docker.model.ManifestRefV2;
import org.apache.hadoop.runc.docker.model.ManifestV2;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.junit.Assert.assertEquals;

public class TestDockerClient {

  static final Logger LOG = LoggerFactory.getLogger(TestDockerClient.class);

  static final String CONFIG_MEDIA_TYPE
      = "application/vnd.docker.container.image.v1+json";

  static final String LAYER_MEDIA_TYPE
      = "application/vnd.docker.image.rootfs.diff.tar.gzip";

  static final String MANIFEST_DIGEST =
      "sha256:a4c8f3631a7d1091064fd7c7d2032688b0e547fc903092cc81ee1b33a77d161f";

  static final String CONFIG_DIGEST =
      "sha256:73b36cf85155a1c2fe3c1d2c7f7b3c78e1d060c4984a5bc5ad019e51e7f6460e";

  static final String LAYER0_DIGEST =
      "sha256:49d1d42c8dd5d9934484f8122259c16e60e31ce8665cb25fc2f909fa0c1b1521";

  static final String LAYER1_DIGEST =
      "sha256:0ce545d269b61f0f79bc6ba0c088309e474f4753970c66bb91be3cf55c5b392d";

  private Server jetty;
  private ServletContextHandler context;
  private String baseUrl;

  private MessageDigest sha256;

  @Before
  public void setUp() throws Exception {
    sha256 = MessageDigest.getInstance("SHA-256");

    jetty = createJettyServer();
    context = createServletContextHandler(jetty);
    context.addServlet(new ServletHolder(new RepositoryServlet()), "/*");
    jetty.start();
    baseUrl = getBaseUrl(jetty);
  }

  @After
  public void tearDown() throws Exception {
    jetty.stop();
  }

  protected static Server createJettyServer() {
    try {
      Server jetty = new Server(0);
      ((ServerConnector) jetty.getConnectors()[0]).setHost("localhost");
      return jetty;
    } catch (Exception ex) {
      throw new RuntimeException("Could not setup Jetty: " + ex.getMessage(),
          ex);
    }
  }

  protected static ServletContextHandler createServletContextHandler(
      Server jetty) {

    ServletContextHandler context = new ServletContextHandler();
    context.setContextPath("/");
    jetty.setHandler(context);

    return context;
  }

  protected static String getBaseUrl(Server jetty) {
    ServerConnector con = (ServerConnector) jetty.getConnectors()[0];
    return String.format("http://%s:%d/v2/", con.getHost(), con.getLocalPort());
  }

  class RepositoryServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {

      LOG.debug("Request URI: {}", req.getRequestURI());

      String requestUri = req.getRequestURI();
      switch (requestUri) {
      case "/v2/":
        resp.setStatus(200);
        break;
      case "/v2/test/image/manifests/latest":
        ManifestListV2 manifests = createManifests();
        sendJson(resp, 200, manifests.getMediaType(), manifests);
        break;
      case "/v2/test/image/manifests/" + MANIFEST_DIGEST:
        ManifestV2 manifest = createManifest();
        sendJson(resp, 200, manifest.getMediaType(), manifest);
        break;
      case "/v2/test/image/blobs/" + CONFIG_DIGEST:
        Object config = createConfig();
        sendJson(resp, 200, CONFIG_MEDIA_TYPE, config);
        break;
      case "/v2/test/image/blobs/" + LAYER0_DIGEST:
        Object layer0 = createLayer(0);
        sendJson(resp, 200, LAYER_MEDIA_TYPE, layer0);
        break;
      case "/v2/test/image/blobs/" + LAYER1_DIGEST:
        Object layer1 = createLayer(1);
        sendJson(resp, 200, LAYER_MEDIA_TYPE, layer1);
        break;
      default:
        LOG.error("Unexpected URI received: {}", requestUri);
        resp.sendError(404);
      }
    }
  }

  void sendJson(HttpServletResponse resp, int status, String contentType,
      Object json) throws IOException {
    byte[] data = serialize(json);
    resp.setStatus(status);
    resp.setContentType(contentType);
    resp.setContentLength(data.length);
    sha256.reset();
    sha256.digest(data);
    String digest = String.format("sha256:%s",
        Hex.encodeHexString(sha256.digest(data)));
    sha256.reset();
    LOG.debug("Content length: {}", data.length);
    LOG.debug("Digest: {}", digest);
    resp.setHeader("docker-content-digest", digest);
    try (OutputStream os = resp.getOutputStream()) {
      os.write(data);
    }
  }

  byte[] serialize(Object object) {
    try {
      return new ObjectMapper().writer().writeValueAsBytes(object);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  ManifestListV2 createManifests() {
    ManifestListV2 manifests = new ManifestListV2();
    manifests.setMediaType(ManifestListV2.CONTENT_TYPE);
    manifests.setSchemaVersion(2);

    ManifestRefV2 mref = new ManifestRefV2();
    mref.setDigest(MANIFEST_DIGEST);
    mref.setMediaType(ManifestRefV2.CONTENT_TYPE);
    mref.setSize(580);
    manifests.getManifests().add(mref);

    return manifests;
  }

  ManifestV2 createManifest() {
    ManifestV2 manifest = new ManifestV2();
    manifest.setMediaType(ManifestV2.CONTENT_TYPE);
    manifest.setSchemaVersion(2);

    BlobV2 config = new BlobV2();
    config.setMediaType(CONFIG_MEDIA_TYPE);
    config.setSize(15);
    config.setDigest(CONFIG_DIGEST);
    manifest.setConfig(config);

    BlobV2 layer0 = new BlobV2();
    layer0.setMediaType("application/vnd.docker.image.rootfs.diff.tar.gzip");
    layer0.setDigest(LAYER0_DIGEST);
    layer0.setSize(16);
    manifest.getLayers().add(layer0);

    BlobV2 layer1 = new BlobV2();
    layer1.setMediaType("application/vnd.docker.image.rootfs.diff.tar.gzip");
    layer1.setDigest(LAYER1_DIGEST);
    layer1.setSize(16);
    manifest.getLayers().add(layer1);

    return manifest;
  }

  ObjectNode createConfig() {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode root = mapper.createObjectNode();
    root.put("id", "config");
    return root;
  }

  ObjectNode createLayer(int index) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode root = mapper.createObjectNode();
    root.put("id", String.format("layer-%d", index));
    return root;
  }

  @Test
  public void dockerImageShouldBeDownloadable() throws Exception {
    String image = "test/image";
    String tag = "latest";

    try (DockerClient client = new DockerClient()) {
      DockerContext ctx = client.createContext(baseUrl);
      ManifestListV2 manifests = client.listManifests(ctx, image, tag);

      ManifestRefV2 mref = client
          .getManifestChooser()
          .chooseManifest(manifests);

      byte[] manifestData = client
          .readManifest(ctx, image, mref.getDigest());

      ManifestV2 manifest = client.parseManifest(manifestData);

      BlobV2 configRef = manifest.getConfig();

      byte[] configData = client
          .readConfig(ctx, image, configRef.getDigest());

      assertEquals("Wrong config data", "{\"id\":\"config\"}",
          new String(configData, StandardCharsets.UTF_8));

      int layerCount = 0;
      for (BlobV2 layerRef : manifest.getLayers()) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
          try (InputStream in = client
              .download(ctx, image, layerRef.getDigest())) {
            IOUtils.copyBytes(in, out, 1024);
          }
          assertEquals("Wrong layer content",
              String.format("{\"id\":\"layer-%d\"}", layerCount++),
              new String(out.toByteArray(), StandardCharsets.UTF_8));
        }
      }
    }
  }

}
