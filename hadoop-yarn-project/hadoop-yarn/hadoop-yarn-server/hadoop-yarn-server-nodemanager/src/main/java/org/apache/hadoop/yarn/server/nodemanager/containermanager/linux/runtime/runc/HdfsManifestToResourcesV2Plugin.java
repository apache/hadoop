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

import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_NM_RUNC_STAT_CACHE_TIMEOUT;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.DEFAULT_RUNC_STAT_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_IMAGE_TOPLEVEL_DIR;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_STAT_CACHE_SIZE;
import static org.apache.hadoop.yarn.conf.YarnConfiguration.NM_RUNC_STAT_CACHE_TIMEOUT;

/**
 * This class is a V2 plugin that maps runC image manifests into their
 * associated config and layers that are located in HDFS.
 */
@InterfaceStability.Unstable
public class HdfsManifestToResourcesV2Plugin extends AbstractService implements
    RuncManifestToResourcesPlugin {
  private Configuration conf;
  private String layerDir;
  private String configDir;
  private FileSystem fs;
  private LoadingCache<Path, FileStatus> statCache;

  private static final String CONFIG_MEDIA_TYPE =
      "application/vnd.docker.container.image.v1+json";

  private static final String LAYER_TAR_GZIP_MEDIA_TYPE =
      "application/vnd.docker.image.rootfs.diff.tar.gzip";

  private static final String SHA_256 = "sha256";

  private static final String CONFIG_HASH_ALGORITHM =
      SHA_256;

  private static final String LAYER_HASH_ALGORITHM =
      SHA_256;

  private static final String HASH_REGEX = "[a-fA-F0-9]{64}";

  private static final Log LOG = LogFactory.getLog(
      HdfsManifestToResourcesV2Plugin.class);

  public HdfsManifestToResourcesV2Plugin() {
    super(HdfsManifestToResourcesV2Plugin.class.getName());
  }

  @Override
  public void serviceInit(Configuration configuration) {
    this.conf = configuration;
    String toplevelDir = conf.get(NM_RUNC_IMAGE_TOPLEVEL_DIR,
        DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR);
    this.layerDir = toplevelDir + "/layer/";
    this.configDir = toplevelDir + "/config/";
    CacheLoader<Path, FileStatus> cacheLoader =
        new CacheLoader<Path, FileStatus>() {
        @Override
        public FileStatus load(@Nonnull Path path) throws Exception {
          return statBlob(path);
        }
    };
    int statCacheSize = conf.getInt(NM_RUNC_STAT_CACHE_SIZE,
        DEFAULT_RUNC_STAT_CACHE_SIZE);
    int statCacheTimeout = conf.getInt(NM_RUNC_STAT_CACHE_TIMEOUT,
        DEFAULT_NM_RUNC_STAT_CACHE_TIMEOUT);
    this.statCache = CacheBuilder.newBuilder().maximumSize(statCacheSize)
        .refreshAfterWrite(statCacheTimeout, TimeUnit.SECONDS)
        .build(cacheLoader);
  }

  @Override
  public void serviceStart() throws IOException {
    Path path = new Path(layerDir);
    this.fs = path.getFileSystem(conf);
  }

  /**
   * Gets the runc image layer resources.
   *
   * @param manifest The image manifest to get the layers for.
   * @return The list of layers.
   * @throws IOException When it fails to get the layer resources.
   */
  @Override
  public List<LocalResource> getLayerResources(ImageManifest manifest)
      throws IOException  {
    List<LocalResource> localRsrcs = new ArrayList<>();

    for (ImageManifest.Blob blob : manifest.getLayers()) {
      LocalResource rsrc = getResource(blob, layerDir,
          LAYER_TAR_GZIP_MEDIA_TYPE, LAYER_HASH_ALGORITHM, ".sqsh");
      localRsrcs.add(rsrc);
    }

    return localRsrcs;
  }

  /**
   * Gets the runc image config resource.
   *
   * @param manifest The image manifest to get the image config for.
   * @return The image config resource.
   * @throws IOException When it fails to get the image config resource.
   */
  public LocalResource getConfigResource(ImageManifest manifest)
      throws IOException {
    ImageManifest.Blob config = manifest.getConfig();
    return getResource(config, configDir, CONFIG_MEDIA_TYPE,
        CONFIG_HASH_ALGORITHM, "");
  }

  /**
   * Validates and gets runc image config and layer resources.
   *
   * @param blob The ImageManifest blob.
   * @param dir The HDFS resource directory.
   * @param expectedMediaType The expected resource media type.
   * @param expectedHashAlgorithm The expected resource hash algorithm.
   * @param resourceSuffix The resource suffix.
   * @return The runc image resources.
   * @throws IOException When it fails to return the local resources.
   */
  public LocalResource getResource(ImageManifest.Blob blob,
      String dir, String expectedMediaType,
      String expectedHashAlgorithm, String resourceSuffix) throws IOException {
    String mediaType = blob.getMediaType();
    if (!mediaType.equals(expectedMediaType)) {
      throw new IOException("Invalid blob mediaType: " + mediaType);
    }

    if (blob.getDigest() == null) {
      throw new IOException("Invalid blob digest");
    }

    String[] blobDigest = blob.getDigest().split(":", 2);

    String algorithm = blobDigest[0];
    if (!algorithm.equals(expectedHashAlgorithm)) {
      throw new IOException("Invalid blob digest algorithm: " + algorithm);
    }

    String hash = blobDigest[1];
    if (!hash.matches(HASH_REGEX)) {
      throw new IOException("Malformed blob digest: " + hash);
    }

    long size = blob.getSize();
    Path hashPath = new Path(dir, hash.substring(0, 2));
    Path path = new Path(hashPath, hash + resourceSuffix);
    LOG.debug("Getting resource: " + path);
    LocalResource rsrc;

    try {
      FileStatus stat = statCache.get(path);
      long timestamp = stat.getModificationTime();
      URL url = URL.fromPath(path);

      rsrc = LocalResource.newInstance(url,
        LocalResourceType.FILE, LocalResourceVisibility.PUBLIC,
        size, timestamp);
    } catch (ExecutionException e) {
      throw new IOException(e);
    }

    return rsrc;
  }

  /**
   * Gets the file status for a given path.
   *
   * @param path to get the FileStatus for.
   * @return the FileStatus from the given path.
   * @throws IOException When it fails to get the FileStatus for the
   * given path.
   */
  protected FileStatus statBlob(Path path) throws IOException {
    return fs.getFileStatus(path);
  }
}
