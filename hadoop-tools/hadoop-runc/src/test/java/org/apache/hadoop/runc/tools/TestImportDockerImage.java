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

package org.apache.hadoop.runc.tools;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.runc.squashfs.SquashFsReader;
import org.apache.hadoop.runc.squashfs.inode.DirectoryINode;
import org.apache.hadoop.runc.squashfs.inode.FileINode;
import org.apache.hadoop.runc.squashfs.inode.INode;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class TestImportDockerImage {

  static final Logger LOG =
      LoggerFactory.getLogger(TestImportDockerImage.class);

  private static final String MANIFEST_MEDIA_TYPE =
      "application/vnd.docker.distribution.manifest.v2+json";

  private static final String IMAGE_MEDIA_TYPE =
      "application/vnd.docker.container.image.v1+json";

  private static final String LAYER_MEDIA_TYPE =
      "application/vnd.docker.image.rootfs.diff.tar.gzip";

  private Server jetty;
  private ServletContextHandler context;
  private String registryLocation;

  private MessageDigest sha256;

  private File workDir;
  private File dfsDir;

  private ObjectMapper mapper;

  private Map<String, byte[]> repoBlobs = new HashMap<>();
  private Map<String, String> repoHashes = new HashMap<>();
  private Map<String, String> repoTypes = new HashMap<>();

  private byte[] config;
  private byte[] layer;
  private byte[] manifest;

  @Rule
  public TemporaryFolder tmp = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    mapper = new ObjectMapper();

    workDir = tmp.newFolder("work");
    dfsDir = tmp.newFolder("dfs");

    sha256 = MessageDigest.getInstance("SHA-256");

    jetty = createJettyServer();
    context = createServletContextHandler(jetty);
    context.addServlet(new ServletHolder(new RepositoryServlet()), "/*");
    jetty.start();
    registryLocation = getRegistryHostAndPort(jetty);

    LOG.debug("Registry location: {}", registryLocation);

    // create some data
    config = createConfig();
    layer = createLayer();
    manifest = createManifest(config, layer);

    registerObject("/v2/test/image/manifests/latest",
        manifest, MANIFEST_MEDIA_TYPE);
    registerObject("/v2/test/image/manifests/sha256:" + sha256(manifest),
        manifest, MANIFEST_MEDIA_TYPE);
    registerObject("/v2/test/image/blobs/sha256:" + sha256(config),
        config, IMAGE_MEDIA_TYPE);
    registerObject("/v2/test/image/blobs/sha256:" + sha256(layer),
        layer, LAYER_MEDIA_TYPE);
  }

  @After
  public void tearDown() throws Exception {
    LOG.info("teardown");
    jetty.stop();
  }

  void registerObject(String uri, byte[] data, String mediaType) {
    repoBlobs.put(uri, data);
    repoHashes.put(uri, sha256(data));
    repoTypes.put(uri, mediaType);
  }

  byte[] createConfig() throws IOException {
    ObjectNode root = mapper.createObjectNode();
    root.set("config", mapper.createObjectNode());
    return mapper.writer().writeValueAsBytes(root);
  }

  byte[] createLayer() throws IOException {
    ByteArrayOutputStream bos = new ByteArrayOutputStream();
    try (GZIPOutputStream gos = new GZIPOutputStream(bos);
        TarArchiveOutputStream tos = new TarArchiveOutputStream(gos)) {

      // add a directory
      TarArchiveEntry dir = new TarArchiveEntry("dir/");
      dir.setMode((short) 0755);
      dir.setSize(0L);
      tos.putArchiveEntry(dir);
      tos.closeArchiveEntry();

      // add a file
      TarArchiveEntry file = new TarArchiveEntry("dir/file");
      file.setMode((short) 0644);
      file.setSize(4);
      tos.putArchiveEntry(file);
      tos.write("test".getBytes(StandardCharsets.UTF_8));
      tos.closeArchiveEntry();
    }
    bos.flush();
    return bos.toByteArray();
  }

  byte[] createManifest(
      byte[] configData, byte[] layerData) throws IOException {
    ObjectNode root = mapper.createObjectNode();
    root.put("schemaVersion", 2);
    root.put("mediaType", MANIFEST_MEDIA_TYPE);

    ObjectNode cfg = mapper.createObjectNode();
    cfg.put("mediaType", IMAGE_MEDIA_TYPE);
    cfg.put("size", configData.length);
    cfg.put("digest", "sha256:" + sha256(configData));
    root.set("config", cfg);

    ArrayNode layers = mapper.createArrayNode();
    ObjectNode layer0 = mapper.createObjectNode();
    layer0.put("mediaType", LAYER_MEDIA_TYPE);
    layer0.put("size", layerData.length);
    layer0.put("digest", "sha256:" + sha256(layerData));

    layers.add(layer0);
    root.set("layers", layers);

    return mapper.writer().writeValueAsBytes(root);
  }

  String sha256(byte[] data) {
    try {
      MessageDigest digester = MessageDigest.getInstance("SHA-256");
      return Hex.encodeHexString(digester.digest(data));
    } catch (Exception e) {
      throw new RuntimeException("Error in sha256", e);
    }
  }

  protected static Server createJettyServer() {
    try {
      Server jetty = new Server(0);
      ((ServerConnector) jetty.getConnectors()[0]).setHost("127.0.0.1");
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

  protected static String getRegistryHostAndPort(Server jetty) {
    ServerConnector con = (ServerConnector) jetty.getConnectors()[0];
    return String.format("%s:%d", con.getHost(), con.getLocalPort());
  }

  class RepositoryServlet extends HttpServlet {
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws IOException {

      LOG.debug("Request URI: {}", req.getRequestURI());

      String requestUri = req.getRequestURI();
      if ("/v2/".equals(requestUri)) {
        resp.setStatus(200);
        return;
      }

      byte[] blob = repoBlobs.get(requestUri);
      if (blob == null) {
        LOG.error("Unexpected URI received: {}", requestUri);
        resp.sendError(404);
        return;
      }

      String mediaType = repoTypes.get(requestUri);
      String hash = repoHashes.get(requestUri);
      sendObject(resp, mediaType, hash, blob);
    }

    void sendObject(
        HttpServletResponse resp,
        String contentType,
        String hash,
        byte[] data) throws IOException {
      resp.setStatus(200);
      resp.setContentType(contentType);
      resp.setContentLength(data.length);
      String digest = "sha256:" + hash;
      LOG.debug("Content length: {}", data.length);
      LOG.debug("Digest: {}", digest);
      resp.setHeader("docker-content-digest", digest);
      try (OutputStream os = resp.getOutputStream()) {
        os.write(data);
      }
    }
  }

  File repoFile(String relPath) {
    return new File(dfsDir, "runc-repository/" + relPath);
  }

  void assertRepoContent(String path, byte[] expected)
      throws IOException {

    File file = repoFile(path);
    assertTrue("Repo file " + path + " does not exist", file.exists());

    byte[] actual = Files.readAllBytes(file.toPath());

    assertEquals("Wrong hash for " + path, sha256(expected), sha256(actual));
  }

  @Test
  public void testImageShouldConvert() throws Exception {
    Configuration conf = new Configuration();
    conf.set(
        CommonConfigurationKeys.FS_DEFAULT_NAME_KEY,
        dfsDir.toURI().toURL().toExternalForm());
    conf.set(
        YarnConfiguration.NM_RUNC_IMAGE_TOPLEVEL_DIR,
        new File(dfsDir, "runc-repository").toURI().toURL().toExternalForm());

    String src = registryLocation + "/test/image";
    String dest = "test/image";

    ImportDockerImage tool = new ImportDockerImage();
    int result = ToolRunner.run(conf, tool, new String[] {src, dest});
    assertEquals("Tool failed", 0, result);

    // validate metadata file
    File meta = repoFile("meta/test/image@latest.properties");
    assertTrue("Metadata file " + meta + " does not exist", meta.exists());

    Properties props = new Properties();
    try (FileInputStream is = new FileInputStream(meta)) {
      props.load(is);
    }

    assertEquals("Wrong value for runc.import.type",
        "docker", props.getProperty("runc.import.type"));
    assertEquals("Wrong value for runc.import.source",
        src, props.getProperty("runc.import.source"));
    assertEquals("Wrong value for runc.manifest",
        "sha256:" + sha256(manifest), props.getProperty("runc.manifest"));
    assertNotNull("Missing value for runc.import.time",
        props.getProperty("runc.import.time"));

    // validate manifest matches
    String manifestPath = String.format("manifest/%s/%s",
        sha256(manifest).substring(0, 2), sha256(manifest));
    assertRepoContent(manifestPath, manifest);

    // validate config matches
    String configPath = String.format("config/%s/%s",
        sha256(config).substring(0, 2), sha256(config));
    assertRepoContent(configPath, config);

    // validate original tar.gz matches
    String layerPath = String.format("layer/%s/%s.tar.gz",
        sha256(layer).substring(0, 2), sha256(layer));
    assertRepoContent(layerPath, layer);

    String sqshPath = String.format("layer/%s/%s.sqsh",
        sha256(layer).substring(0, 2), sha256(layer));
    File sqsh = repoFile(sqshPath);

    assertTrue("SquashFS file " + sqsh + " does not exist", sqsh.exists());

    try (SquashFsReader reader = SquashFsReader.fromFile(sqsh)) {
      INode dir = reader.findInodeByPath("/dir");
      assertTrue("Dir is not a directory: " + dir.getClass().getName(),
          dir instanceof DirectoryINode);

      INode file = reader.findInodeByPath("/dir/file");
      assertTrue("File is not a file: " + file.getClass().getName(),
          file instanceof FileINode);

      FileINode fInode = (FileINode) file;

      assertEquals("Wrong file length", 4, fInode.getFileSize());

      byte[] buf = new byte[4];
      reader.read(file, 0L, buf, 0, 4);
      assertEquals("Wrong file data", "test",
          new String(buf, StandardCharsets.UTF_8));
    }
  }

}
