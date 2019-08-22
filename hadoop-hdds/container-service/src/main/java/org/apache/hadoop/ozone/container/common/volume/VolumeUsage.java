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

package org.apache.hadoop.ozone.container.common.volume;

import com.google.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CachingGetSpaceUsed;
import org.apache.hadoop.fs.DF;
import org.apache.hadoop.fs.GetSpaceUsed;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Scanner;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Class that wraps the space df of the Datanode Volumes used by SCM
 * containers.
 */
public class VolumeUsage {
  private static final Logger LOG = LoggerFactory.getLogger(VolumeUsage.class);

  private final File rootDir;
  private final DF df;
  private final File scmUsedFile;
  private AtomicReference<GetSpaceUsed> scmUsage;
  private boolean shutdownComplete;

  private static final String DU_CACHE_FILE = "scmUsed";
  private volatile boolean scmUsedSaved = false;

  VolumeUsage(File dataLoc, Configuration conf)
      throws IOException {
    this.rootDir = dataLoc;

    // SCM used cache file
    scmUsedFile = new File(rootDir, DU_CACHE_FILE);
    // get overall disk df
    this.df = new DF(rootDir, conf);

    startScmUsageThread(conf);
  }

  void startScmUsageThread(Configuration conf) throws IOException {
    // get SCM specific df
    scmUsage = new AtomicReference<>(
        new CachingGetSpaceUsed.Builder().setPath(rootDir)
            .setConf(conf)
            .setInitialUsed(loadScmUsed())
            .build());
  }

  long getCapacity() {
    long capacity = df.getCapacity();
    return (capacity > 0) ? capacity : 0;
  }

  /*
   * Calculate the available space in the volume.
   */
  long getAvailable() throws IOException {
    long remaining = getCapacity() - getScmUsed();
    long available = df.getAvailable();
    if (remaining > available) {
      remaining = available;
    }
    return (remaining > 0) ? remaining : 0;
  }

  long getScmUsed() throws IOException{
    return scmUsage.get().getUsed();
  }

  public synchronized void shutdown() {
    if (!shutdownComplete) {
      saveScmUsed();

      if (scmUsage.get() instanceof CachingGetSpaceUsed) {
        IOUtils.cleanupWithLogger(
            null, ((CachingGetSpaceUsed) scmUsage.get()));
      }
      shutdownComplete = true;
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
        LOG.info("Cached ScmUsed found for {} : {} ", rootDir,
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
      LOG.warn("Failed to delete old scmUsed file in {}.", rootDir);
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

  /**
   * Only for testing. Do not use otherwise.
   */
  @VisibleForTesting
  @SuppressFBWarnings(
      value = "IS2_INCONSISTENT_SYNC",
      justification = "scmUsage is an AtomicReference. No additional " +
          "synchronization is needed.")
  public void setScmUsageForTesting(GetSpaceUsed scmUsageForTest) {
    scmUsage.set(scmUsageForTest);
  }
}
