/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.runc;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_MANIFEST_CACHE_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_IMAGE_META_NAMESPACE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NUM_MANIFESTS_TO_CACHE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_META_NAMESPACE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_MANIFEST_CACHE_ENABLED;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_NUM_MANIFESTS_TO_CACHE;

/**
 * This class is a V2 plugin to convert image tags into runC image manifests.
 */
@InterfaceStability.Unstable
public class ImageTagToManifestV2Plugin extends AbstractService
    implements RuncImageTagToManifestPlugin {

  private Map<String, ImageManifest> manifestCache;
  private ObjectMapper objMapper;
  private Configuration conf;
  private String manifestDir;
  private String metaDir;
  private String defaultMetaNamespace;
  private boolean manifestCacheEnabled;

  private static final String MANIFEST_PREFIX = "runc.manifest";

  private static final String HASH_REGEX = "[a-fA-F0-9]{64}";

  private static final Log LOG = LogFactory.getLog(
      ImageTagToManifestV2Plugin.class);

  public ImageTagToManifestV2Plugin() {
    super("ImageTagToManifestPluginService");
  }

  /**
   * Gets the runc image manifest from cache or HDFS using the specified
   * imageTag.
   *
   * @param imageTag User defined imageTag.
   * @return the ImageManifest.
   * @throws IOException When it is unable to read the manifest file from HDFS.
   */
  @Override
  public ImageManifest getManifestFromImageTag(String imageTag)
      throws IOException {
    LOG.debug("Getting manifest for imageTag: " + imageTag);

    if (imageTag == null || imageTag.isEmpty()) {
      throw new IOException("Unable to read the HDFS manifest file for a null "
          + "or empty imageTag");
    }

    ImageManifest manifest;
    if (manifestCacheEnabled) {
      manifest = manifestCache.get(imageTag);
      if (manifest != null) {
        LOG.debug("Retrieving the manifest from the image manifest cache");
        return manifest;
      }
    }

    String hash;
    try {
      hash = getHashFromImageTag(imageTag);
    } catch (IllegalArgumentException iae) {
      throw new IOException("Unable to read the HDFS manifest file, "
          + "invalid imageTag");
    }

    if (hash == null) {
      throw new IOException("Unable to read the hash for imageTag: "
          + imageTag);
    }

    FSDataInputStream input;
    try {
      Path manifestHashPath = new Path(manifestDir, hash.substring(0, 2));
      Path manifestPath = new Path(manifestHashPath, hash);
      FileSystem fs = manifestPath.getFileSystem(conf);
      input = fs.open(manifestPath);
    } catch (IllegalArgumentException iae) {
      throw new IOException("Failed to read the HDFS manifest file for: "
          + imageTag, iae);
    }

    byte[] bytes = IOUtils.toByteArray(input);
    manifest = objMapper.readValue(bytes, ImageManifest.class);

    if (manifestCacheEnabled) {
      manifestCache.put(imageTag, manifest);
    }

    return manifest;
  }

  /**
   * Gets the runc image hash from the imageTag property file.
   *
   * @param imageTag User defined imageTag.
   * @return the hash of the imageTag.
   */
  @Override
  public String getHashFromImageTag(String imageTag) {
    String hash = null;
    ImageMetadata imageMetadata =
        new ImageMetadata(imageTag, defaultMetaNamespace);
    String metaImageTag = imageMetadata.getNameTag();
    String metaImageNamespace = imageMetadata.getMetaNamespace();
    try {
      BufferedReader br =
          getHdfsImageToHashReader(metaImageTag, metaImageNamespace);
      hash = readImageToHashFile(br);
    } catch (IOException e) {
      LOG.error("Failed to read the image hash from the image "
          + "properties file", e);
    }

    return hash;
  }

  /**
   * Gets the HDFS image BufferedReader for the runc image property file. The
   * image property file contains a special delimiter to match the runc image
   * import CLI tool.
   *
   * @param imageTag Validated user defined imageTag.
   * @param imageNamespace Validated image namespace.
   * @return The BufferedReader for the image property file.
   * @throws IOException when it is unable to load the HDFS file.
   */
  protected BufferedReader getHdfsImageToHashReader(String imageTag,
      String imageNamespace) throws IOException {
    String updatedImageTag = imageTag.replace(':', '@');
    String imageFile = metaDir + imageNamespace + "/" + updatedImageTag
        + ".properties";
    LOG.debug("Checking HDFS for image file: " + imageFile);
    Path propertiesFile = new Path(imageFile);
    FileSystem fs = propertiesFile.getFileSystem(conf);
    if (!fs.exists(propertiesFile)) {
      LOG.warn("Did not load the hdfs image to hash properties file, "
          + "file doesn't exist");
      return null;
    }

    return new BufferedReader(new InputStreamReader(fs.open(propertiesFile),
        StandardCharsets.UTF_8));
  }

  /**
   * Read the image properties file to get the hash from the manifest
   * prefix line.  Other image metadata lines are optional and are ignored
   * for now.
   *
   * @param br The HDFS image hash reader for the image property file.
   * @return The manifest hash.
   * @throws IOException when unable to read the hash.
   */
  protected static String readImageToHashFile(BufferedReader br)
      throws IOException {
    if (br == null) {
      return null;
    }

    String hash = null;
    String line;
    while ((line = br.readLine()) != null) {
      if (line.startsWith(MANIFEST_PREFIX)) {
        if (line.contains(":")) {
          String[] parts = line.split(":");
          hash = parts[1];
        } else {
          return null;
        }

        if (!hash.matches(HASH_REGEX)) {
          LOG.warn("Malformed image hash: " + hash);
        }
      }
    }

    return hash;
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    super.serviceInit(configuration);
    this.conf = configuration;
    this.objMapper = new ObjectMapper();
    manifestDir = conf.get(NM_RUNC_IMAGE_TOPLEVEL_DIR,
        DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR) + "/manifest/";
    metaDir = conf.get(NM_RUNC_IMAGE_TOPLEVEL_DIR,
        DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR) + "/meta/";
    defaultMetaNamespace = conf.get(NM_RUNC_IMAGE_META_NAMESPACE,
        DEFAULT_NM_RUNC_IMAGE_META_NAMESPACE);
    manifestCacheEnabled = conf.getBoolean(NM_RUNC_MANIFEST_CACHE_ENABLED,
        DEFAULT_NM_RUNC_MANIFEST_CACHE_ENABLED);

    if (manifestCacheEnabled) {
      LOG.debug("The image manifest cache is enabled");
      int numManifestsToCache = conf.getInt(NM_RUNC_NUM_MANIFESTS_TO_CACHE,
          DEFAULT_NUM_MANIFESTS_TO_CACHE);
      this.manifestCache = Collections.synchronizedMap(
          new LRUCache(numManifestsToCache, 0.75f));
    }
  }

  private static class LRUCache extends LinkedHashMap<String, ImageManifest> {
    private final int cacheSize;

    LRUCache(int initialCapacity, float loadFactor) {
      super(initialCapacity, loadFactor, true);
      this.cacheSize = initialCapacity;
    }

    @Override
    protected boolean removeEldestEntry(
        Map.Entry<String, ImageManifest> eldest) {
      return this.size() > cacheSize;
    }
  }
}
