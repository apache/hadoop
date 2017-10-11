/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
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

package org.apache.hadoop.ozone.container.common.impl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.hdfs.server.datanode.StorageLocation;
import org.apache.hadoop.hdfs.server.protocol.DatanodeStorage;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ozone.OzoneConsts;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.io.OutputStreamWriter;
import java.io.FileOutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Scanner;

import static org.apache.hadoop.util.RunJar.SHUTDOWN_HOOK_PRIORITY;

/**
 * Class that wraps the space usage of the Datanode Container Storage Location
 * by SCM containers.
 */
public class ContainerStorageLocation {
  private static final Logger LOG =
      LoggerFactory.getLogger(ContainerStorageLocation.class);

  private static final String DU_CACHE_FILE = "scmUsed";
  private volatile boolean scmUsedSaved = false;

  private final StorageLocation dataLocation;
  private final String storageUuId;
  private final DF usage;
  private final GetSpaceUsed scmUsage;
  private final File scmUsedFile;

  public ContainerStorageLocation(StorageLocation dataLoc, Configuration conf)
      throws IOException {
    this.dataLocation = dataLoc;
    this.storageUuId = DatanodeStorage.generateUuid();
    File dataDir = Paths.get(dataLoc.getNormalizedUri()).resolve(
        OzoneConsts.CONTAINER_PREFIX).toFile();
    // Initialize container data root if it does not exist as required by DF/DU
    if (!dataDir.exists()) {
      if (!dataDir.mkdirs()) {
        LOG.error("Unable to create the container storage location at : {}",
            dataDir);
        throw new IllegalArgumentException("Unable to create the container" +
            " storage location at : " + dataDir);
      }
    }
    scmUsedFile = new File(dataDir, DU_CACHE_FILE);
    // get overall disk usage
    this.usage = new DF(dataDir, conf);
    // get SCM specific usage
    this.scmUsage = new CachingGetSpaceUsed.Builder().setPath(dataDir)
        .setConf(conf)
        .setInitialUsed(loadScmUsed())
        .build();

    // Ensure scm usage is saved during shutdown.
    ShutdownHookManager.get().addShutdownHook(
        new Runnable() {
          @Override
          public void run() {
            if (!scmUsedSaved) {
              saveScmUsed();
            }
          }
        }, SHUTDOWN_HOOK_PRIORITY);
  }

  public URI getNormalizedUri() {
    return dataLocation.getNormalizedUri();
  }

  public String getStorageUuId() {
    return storageUuId;
  }
  public long getCapacity() {
    long capacity = usage.getCapacity();
    return (capacity > 0) ? capacity : 0;
  }

  public long getAvailable() throws IOException {
    long remaining = getCapacity() - getScmUsed();
    long available = usage.getAvailable();
    if (remaining > available) {
      remaining = available;
    }
    return (remaining > 0) ? remaining : 0;
  }

  public long getScmUsed() throws IOException{
    return scmUsage.getUsed();
  }

  public void shutdown() {
    saveScmUsed();
    scmUsedSaved = true;

    if (scmUsage instanceof CachingGetSpaceUsed) {
      IOUtils.cleanupWithLogger(null, ((CachingGetSpaceUsed) scmUsage));
    }
  }

  /**
   * Read in the cached DU value and return it if it is less than 600 seconds
   * old (DU update interval). Slight imprecision of scmUsed is not critical
   * and skipping DU can significantly shorten the startup time.
   * If the cached value is not available or too old, -1 is returned.
   */
  long loadScmUsed() {
    long cachedScmUsed;
    long mtime;
    Scanner sc;

    try {
      sc = new Scanner(scmUsedFile, "UTF-8");
    } catch (FileNotFoundException fnfe) {
      return -1;
    }

    try {
      // Get the recorded scmUsed from the file.
      if (sc.hasNextLong()) {
        cachedScmUsed = sc.nextLong();
      } else {
        return -1;
      }
      // Get the recorded mtime from the file.
      if (sc.hasNextLong()) {
        mtime = sc.nextLong();
      } else {
        return -1;
      }

      // Return the cached value if mtime is okay.
      if (mtime > 0 && (Time.now() - mtime < 600000L)) {
        LOG.info("Cached ScmUsed found for {} : {} ", dataLocation,
            cachedScmUsed);
        return cachedScmUsed;
      }
      return -1;
    } finally {
      sc.close();
    }
  }

  /**
   * Write the current scmUsed to the cache file.
   */
  void saveScmUsed() {
    if (scmUsedFile.exists() && !scmUsedFile.delete()) {
      LOG.warn("Failed to delete old scmUsed file in {}.", dataLocation);
    }
    OutputStreamWriter out = null;
    try {
      long used = getScmUsed();
      if (used > 0) {
        out = new OutputStreamWriter(new FileOutputStream(scmUsedFile),
            StandardCharsets.UTF_8);
        // mtime is written last, so that truncated writes won't be valid.
        out.write(Long.toString(used) + " " + Long.toString(Time.now()));
        out.flush();
        out.close();
        out = null;
      }
    } catch (IOException ioe) {
      // If write failed, the volume might be bad. Since the cache file is
      // not critical, log the error and continue.
      LOG.warn("Failed to write scmUsed to " + scmUsedFile, ioe);
    } finally {
      IOUtils.cleanupWithLogger(null, out);
    }
  }
}
