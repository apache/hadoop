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
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.concurrent.HadoopExecutors;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_CACHE_REFRESH_INTERVAL;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NUM_MANIFESTS_TO_CACHE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_HDFS_RUNC_IMAGE_TAG_TO_HASH_FILE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_LOCAL_RUNC_IMAGE_TAG_TO_HASH_FILE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_CACHE_REFRESH_INTERVAL;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_NUM_MANIFESTS_TO_CACHE;

/**
 * This class is a plugin for the
 * {@link org.apache.hadoop.yarn.server.nodemanager.containermanager.linux.runtime.RuncContainerRuntime}
 * to convert image tags into runC image manifests.
 */
@InterfaceStability.Unstable
public class ImageTagToManifestPlugin extends AbstractService
    implements RuncImageTagToManifestPlugin {

  private Map<String, ImageManifest> manifestCache;
  private ObjectMapper objMapper;
  private AtomicReference<Map<String, String>> localImageToHashCache =
      new AtomicReference<>(new HashMap<>());
  private AtomicReference<Map<String, String>> hdfsImageToHashCache =
      new AtomicReference<>(new HashMap<>());
  private Configuration conf;
  private ScheduledExecutorService exec;
  private long hdfsModTime;
  private long localModTime;
  private String hdfsImageToHashFile;
  private String manifestDir;
  private String localImageTagToHashFile;

  private static final Logger LOG = LoggerFactory.getLogger(ImageTagToManifestPlugin.class);

  private static final int SHA256_HASH_LENGTH = 64;
  private static final String ALPHA_NUMERIC = "[a-zA-Z0-9]+";

  public ImageTagToManifestPlugin() {
    super("ImageTagToManifestPluginService");
  }

  @Override
  public ImageManifest getManifestFromImageTag(String imageTag)
      throws IOException {
    String hash = getHashFromImageTag(imageTag);
    ImageManifest manifest = manifestCache.get(hash);
    if (manifest != null) {
      return manifest;
    }

    Path manifestPath = new Path(manifestDir + hash);
    FileSystem fs = manifestPath.getFileSystem(conf);
    FSDataInputStream input;
    try {
      input = fs.open(manifestPath);
    } catch (IllegalArgumentException iae) {
      throw new IOException("Manifest file is not a valid HDFS file: "
          + manifestPath.toString(), iae);
    }

    byte[] bytes = IOUtils.toByteArray(input);
    manifest = objMapper.readValue(bytes, ImageManifest.class);

    manifestCache.put(hash, manifest);
    return manifest;
  }

  @Override
  public String getHashFromImageTag(String imageTag) {
    String hash;
    Map<String, String> localImageToHashCacheMap = localImageToHashCache.get();
    Map<String, String> hdfsImageToHashCacheMap = hdfsImageToHashCache.get();

    // 1) Go to local file
    // 2) Go to HDFS
    // 3) Use tag as is/Assume tag is the hash
    hash = localImageToHashCacheMap.get(imageTag);
    if (hash == null) {
      hash = hdfsImageToHashCacheMap.get(imageTag);
      if (hash == null) {
        hash = imageTag;
      }
    }
    return hash;
  }

  protected BufferedReader getLocalImageToHashReader() throws IOException {
    if (localImageTagToHashFile == null) {
      LOG.debug("Did not load local image to hash file, " +
          "file is null");
      return null;
    }

    File imageTagToHashFile = new File(localImageTagToHashFile);
    if(!imageTagToHashFile.exists()) {
      LOG.debug("Did not load local image to hash file, " +
          "file doesn't exist");
      return null;
    }

    long newLocalModTime = imageTagToHashFile.lastModified();
    if (newLocalModTime == localModTime) {
      LOG.debug("Did not load local image to hash file, " +
          "file is unmodified");
      return null;
    }
    localModTime = newLocalModTime;

    return new BufferedReader(new InputStreamReader(
        new FileInputStream(imageTagToHashFile), StandardCharsets.UTF_8));
  }

  protected BufferedReader getHdfsImageToHashReader() throws IOException {
    if (hdfsImageToHashFile == null) {
      LOG.debug("Did not load hdfs image to hash file, " +
          "file is null");
      return null;
    }

    Path imageToHash = new Path(hdfsImageToHashFile);
    FileSystem fs = imageToHash.getFileSystem(conf);
    if (!fs.exists(imageToHash)) {
      LOG.debug("Did not load hdfs image to hash file, " +
          "file doesn't exist");
      return null;
    }

    long newHdfsModTime = fs.getFileStatus(imageToHash).getModificationTime();
    if (newHdfsModTime == hdfsModTime) {
      LOG.debug("Did not load hdfs image to hash file, " +
          "file is unmodified");
      return null;
    }
    hdfsModTime = newHdfsModTime;

    return new BufferedReader(new InputStreamReader(fs.open(imageToHash),
        StandardCharsets.UTF_8));
  }

  /** You may specify multiple tags per hash all on the same line.
   * Comments are allowed using #. Anything after this character will not
   * be read
   * Example file:
   * foo/bar:current,fizz/gig:latest:123456789
   * #this/line:wont,be:parsed:2378590895

   * This will map both foo/bar:current and fizz/gig:latest to 123456789
   */
  protected static Map<String, String> readImageToHashFile(
      BufferedReader br) throws IOException {
    if (br == null) {
      return null;
    }

    String line;
    Map<String, String> imageToHashCache = new HashMap<>();
    while ((line = br.readLine()) != null) {
      int index;
      index = line.indexOf("#");
      if (index == 0) {
        continue;
      } else if (index != -1) {
        line = line.substring(0, index);
      }

      index = line.lastIndexOf(":");
      if (index == -1) {
        LOG.warn("Malformed imageTagToManifest entry: " + line);
        continue;
      }
      String imageTags = line.substring(0, index);
      String[] imageTagArray = imageTags.split(",");
      String hash = line.substring(index + 1);
      if (!hash.matches(ALPHA_NUMERIC) || hash.length() != SHA256_HASH_LENGTH) {
        LOG.warn("Malformed image hash: " + hash);
        continue;
      }

      for (String imageTag : imageTagArray) {
        imageToHashCache.put(imageTag, hash);
      }
    }
    return imageToHashCache;
  }

  public boolean loadImageToHashFiles() throws IOException {
    boolean ret = false;
    try (
        BufferedReader localBr = getLocalImageToHashReader();
        BufferedReader hdfsBr = getHdfsImageToHashReader()
    ) {
      Map<String, String> localImageToHash = readImageToHashFile(localBr);
      Map<String, String> hdfsImageToHash = readImageToHashFile(hdfsBr);

      Map<String, String> tmpLocalImageToHash = localImageToHashCache.get();
      Map<String, String> tmpHdfsImageToHash = hdfsImageToHashCache.get();

      if (localImageToHash != null &&
          !localImageToHash.equals(tmpLocalImageToHash)) {
        localImageToHashCache.set(localImageToHash);
        LOG.info("Reloaded local image tag to hash cache");
        ret = true;
      }
      if (hdfsImageToHash != null &&
          !hdfsImageToHash.equals(tmpHdfsImageToHash)) {
        hdfsImageToHashCache.set(hdfsImageToHash);
        LOG.info("Reloaded hdfs image tag to hash cache");
        ret = true;
      }
    }
    return ret;
  }

  @Override
  protected void serviceInit(Configuration configuration) throws Exception {
    super.serviceInit(configuration);
    this.conf = configuration;
    localImageTagToHashFile = conf.get(NM_LOCAL_RUNC_IMAGE_TAG_TO_HASH_FILE);
    if (localImageTagToHashFile == null) {
      LOG.debug("Failed to load local runC image to hash file. " +
          "Config not set");
    }
    hdfsImageToHashFile = conf.get(NM_HDFS_RUNC_IMAGE_TAG_TO_HASH_FILE);
    if (hdfsImageToHashFile == null) {
      LOG.debug("Failed to load HDFS runC image to hash file. Config not set");
    }
    if(hdfsImageToHashFile == null && localImageTagToHashFile == null) {
      LOG.warn("No valid image-tag-to-hash files");
    }
    manifestDir = conf.get(NM_RUNC_IMAGE_TOPLEVEL_DIR,
        DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR) + "/manifests/";
    int numManifestsToCache = conf.getInt(NM_RUNC_NUM_MANIFESTS_TO_CACHE,
        DEFAULT_NUM_MANIFESTS_TO_CACHE);
    this.objMapper = new ObjectMapper();
    this.manifestCache = Collections.synchronizedMap(
        new LRUCache(numManifestsToCache, 0.75f));

    exec = HadoopExecutors.newScheduledThreadPool(1);
  }

  @Override
  protected void serviceStart() throws Exception {
    super.serviceStart();
    if(!loadImageToHashFiles()) {
      LOG.warn("Couldn't load any image-tag-to-hash-files");
    }
    int runcCacheRefreshInterval = conf.getInt(NM_RUNC_CACHE_REFRESH_INTERVAL,
        DEFAULT_NM_RUNC_CACHE_REFRESH_INTERVAL);
    exec = HadoopExecutors.newScheduledThreadPool(1);
    exec.scheduleWithFixedDelay(
        new Runnable() {
          @Override
          public void run() {
            try {
              loadImageToHashFiles();
            } catch (Exception e) {
              LOG.warn("runC cache refresh thread caught an exception: ", e);
            }
          }
        }, runcCacheRefreshInterval, runcCacheRefreshInterval, TimeUnit.SECONDS);
  }

  @Override
  protected void serviceStop() throws Exception {
    super.serviceStop();
    exec.shutdownNow();
  }

  private static class LRUCache extends LinkedHashMap<String, ImageManifest> {
    private int cacheSize;

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
