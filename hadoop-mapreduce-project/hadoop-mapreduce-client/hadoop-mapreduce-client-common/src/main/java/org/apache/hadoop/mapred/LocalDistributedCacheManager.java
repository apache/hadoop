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

package org.apache.hadoop.mapred;

import com.google.common.collect.Maps;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.v2.util.MRApps;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.FSDownload;

/**
 * A helper class for managing the distributed cache for {@link LocalJobRunner}.
 */
@SuppressWarnings("deprecation")
class LocalDistributedCacheManager {
  public static final Log LOG =
    LogFactory.getLog(LocalDistributedCacheManager.class);
  
  private List<String> localArchives = new ArrayList<String>();
  private List<String> localFiles = new ArrayList<String>();
  private List<String> localClasspaths = new ArrayList<String>();
  
  private boolean setupCalled = false;
  
  /**
   * Set up the distributed cache by localizing the resources, and updating
   * the configuration with references to the localized resources.
   * @param conf
   * @throws IOException
   */
  public void setup(JobConf conf) throws IOException {
    // Generate YARN local resources objects corresponding to the distributed
    // cache configuration
    Map<String, LocalResource> localResources = 
      new LinkedHashMap<String, LocalResource>();
    MRApps.setupDistributedCache(conf, localResources);
    
    // Find which resources are to be put on the local classpath
    Map<String, Path> classpaths = new HashMap<String, Path>();
    Path[] archiveClassPaths = DistributedCache.getArchiveClassPaths(conf);
    if (archiveClassPaths != null) {
      for (Path p : archiveClassPaths) {
        FileSystem remoteFS = p.getFileSystem(conf);
        p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
            remoteFS.getWorkingDirectory()));
        classpaths.put(p.toUri().getPath().toString(), p);
      }
    }
    Path[] fileClassPaths = DistributedCache.getFileClassPaths(conf);
    if (fileClassPaths != null) {
      for (Path p : fileClassPaths) {
        FileSystem remoteFS = p.getFileSystem(conf);
        p = remoteFS.resolvePath(p.makeQualified(remoteFS.getUri(),
            remoteFS.getWorkingDirectory()));
        classpaths.put(p.toUri().getPath().toString(), p);
      }
    }
    
    // Localize the resources
    LocalDirAllocator localDirAllocator =
      new LocalDirAllocator(MRConfig.LOCAL_DIR);
    FileContext localFSFileContext = FileContext.getLocalFSFileContext();
    UserGroupInformation ugi = UserGroupInformation.getCurrentUser();
    
    Map<LocalResource, Future<Path>> resourcesToPaths = Maps.newHashMap();
    ExecutorService exec = Executors.newCachedThreadPool();
    Path destPath = localDirAllocator.getLocalPathForWrite(".", conf);
    for (LocalResource resource : localResources.values()) {
      Callable<Path> download = new FSDownload(localFSFileContext, ugi, conf,
          destPath, resource, new Random());
      Future<Path> future = exec.submit(download);
      resourcesToPaths.put(resource, future);
    }
    for (LocalResource resource : localResources.values()) {
      Path path;
      try {
        path = resourcesToPaths.get(resource).get();
      } catch (InterruptedException e) {
        throw new IOException(e);
      } catch (ExecutionException e) {
        throw new IOException(e);
      }
      String pathString = path.toUri().toString();
      if (resource.getType() == LocalResourceType.ARCHIVE) {
        localArchives.add(pathString);
      } else if (resource.getType() == LocalResourceType.FILE) {
        localFiles.add(pathString);
      }
      Path resourcePath;
      try {
        resourcePath = ConverterUtils.getPathFromYarnURL(resource.getResource());
      } catch (URISyntaxException e) {
        throw new IOException(e);
      }
      LOG.info(String.format("Localized %s as %s", resourcePath, path));
      String cp = resourcePath.toUri().getPath();
      if (classpaths.keySet().contains(cp)) {
        localClasspaths.add(path.toUri().getPath().toString());
      }
    }
    
    // Update the configuration object with localized data.
    if (!localArchives.isEmpty()) {
      conf.set(MRJobConfig.CACHE_LOCALARCHIVES, StringUtils
          .arrayToString(localArchives.toArray(new String[localArchives
              .size()])));
    }
    if (!localFiles.isEmpty()) {
      conf.set(MRJobConfig.CACHE_LOCALFILES, StringUtils
          .arrayToString(localFiles.toArray(new String[localArchives
              .size()])));
    }
    if (DistributedCache.getSymlink(conf)) {
      // This is not supported largely because, 
      // for a Child subprocess, the cwd in LocalJobRunner
      // is not a fresh slate, but rather the user's working directory.
      // This is further complicated because the logic in
      // setupWorkDir only creates symlinks if there's a jarfile
      // in the configuration.
      LOG.warn("LocalJobRunner does not support " +
          "symlinking into current working dir.");
    }
    setupCalled = true;
  }

  /** 
   * Are the resources that should be added to the classpath? 
   * Should be called after setup().
   * 
   */
  public boolean hasLocalClasspaths() {
    if (!setupCalled) {
      throw new IllegalStateException(
          "hasLocalClasspaths() should be called after setup()");
    }
    return !localClasspaths.isEmpty();
  }
  
  /**
   * Creates a class loader that includes the designated
   * files and archives.
   */
  public ClassLoader makeClassLoader(final ClassLoader parent)
      throws MalformedURLException {
    final URL[] urls = new URL[localClasspaths.size()];
    for (int i = 0; i < localClasspaths.size(); ++i) {
      urls[i] = new File(localClasspaths.get(i)).toURI().toURL();
      LOG.info(urls[i]);
    }
    return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>() {
      @Override
      public ClassLoader run() {
        return new URLClassLoader(urls, parent);
      }
    });
  }

  public void close() throws IOException {
    FileContext localFSFileContext = FileContext.getLocalFSFileContext();
    for (String archive : localArchives) {
      localFSFileContext.delete(new Path(archive), true);
    }
    for (String file : localFiles) {
      localFSFileContext.delete(new Path(file), true);
    }
  }
}
