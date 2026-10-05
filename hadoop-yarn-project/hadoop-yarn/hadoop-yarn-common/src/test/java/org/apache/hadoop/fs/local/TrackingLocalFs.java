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
package org.apache.hadoop.fs.local;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.EnumSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Options.ChecksumOpt;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * Test {@link LocalFs} that tracks open output streams and injects I/O failures.
 *
 * <p>Create failures: path suffix match on {@link #createInternal}. Flush failures:
 * {@link #setFailOnFlushExcludingPathSuffix(String, int)} counts only streams
 * whose path name does <em>not</em> end with the given suffix (e.g. skip checksum
 * side files during rolling aggregation).
 *
 * <p>In {@code org.apache.hadoop.fs.local} for the package-private URI constructor.
 * Register as {@code fs.AbstractFileSystem.file.impl}.
 *
 * <p>Uses JVM-wide static state; not safe under parallel test execution unless tests
 * acquire {@link #RESOURCE_LOCK} (see {@code @ResourceLock}).
 */
public class TrackingLocalFs extends LocalFs {

  /** JUnit {@code @ResourceLock} name for serializing tests that use this class. */
  public static final String RESOURCE_LOCK =
      "org.apache.hadoop.fs.local.TrackingLocalFs";

  /** Output streams returned from {@link #createInternal} that remain open. */
  public static final Set<FSDataOutputStream> OPEN_STREAMS =
      ConcurrentHashMap.newKeySet();

  /** {@link #createInternal} calls since {@link #reset()}. */
  public static final AtomicInteger CREATE_CALLS = new AtomicInteger();

  private static final AtomicBoolean FAILURE_INJECTED = new AtomicBoolean();
  private static volatile String failOnPathSuffix = null;
  /** When set, only non-matching paths participate in scoped flush counting. */
  private static volatile String failOnFlushExcludeSuffix = null;
  /** 1-based scoped {@code flush} ordinal; {@code -1} disables. */
  private static volatile int failOnScopedFlushCall = -1;
  private static final AtomicInteger SCOPED_FLUSH_CALLS = new AtomicInteger();

  /** Fail the next {@link #createInternal} whose path name ends with {@code suffix}. */
  public static void setFailOnPathSuffix(String suffix) {
    failOnPathSuffix = suffix;
  }

  /**
   * Fail on the {@code nthFlush}-th {@code flush()} of streams whose path name
   * does not end with {@code excludeSuffix}.
   */
  public static void setFailOnFlushExcludingPathSuffix(
      String excludeSuffix, int nthFlush) {
    failOnFlushExcludeSuffix = excludeSuffix;
    failOnScopedFlushCall = nthFlush;
  }

  /** Whether an injected create or flush failure has been triggered. */
  public static boolean wasFailureInjected() {
    return FAILURE_INJECTED.get();
  }

  /** Clears counters, open-stream set, and injection settings. */
  public static void reset() {
    OPEN_STREAMS.clear();
    CREATE_CALLS.set(0);
    SCOPED_FLUSH_CALLS.set(0);
    FAILURE_INJECTED.set(false);
    failOnPathSuffix = null;
    failOnFlushExcludeSuffix = null;
    failOnScopedFlushCall = -1;
  }

  public TrackingLocalFs(final URI theUri, final Configuration conf)
      throws IOException, URISyntaxException {
    super(theUri, conf);
  }

  @Override
  public FSDataOutputStream createInternal(Path f,
      EnumSet<CreateFlag> createFlag, FsPermission absolutePermission,
      int bufferSize, short replication, long blockSize, Progressable progress,
      ChecksumOpt checksumOpt, boolean createParent) throws IOException {
    CREATE_CALLS.incrementAndGet();
    if (failOnPathSuffix != null && f.getName().endsWith(failOnPathSuffix)) {
      FAILURE_INJECTED.set(true);
      throw new IOException("Injected createInternal() failure for path " + f);
    }
    FSDataOutputStream real = super.createInternal(f, createFlag,
        absolutePermission, bufferSize, replication, blockSize, progress,
        checksumOpt, createParent);
    final String pathName = f.getName();
    FSDataOutputStream tracked = new FSDataOutputStream(real, null) {
      private final AtomicBoolean closed = new AtomicBoolean();

      @Override
      public void flush() throws IOException {
        if (failOnFlushExcludeSuffix != null
            && !pathName.endsWith(failOnFlushExcludeSuffix)) {
          int flushN = SCOPED_FLUSH_CALLS.incrementAndGet();
          if (flushN == failOnScopedFlushCall) {
            FAILURE_INJECTED.set(true);
            throw new IOException("Injected flush() failure #" + flushN
                + " on path " + pathName);
          }
        }
        super.flush();
      }

      @Override
      public void close() throws IOException {
        try {
          super.close();
        } finally {
          if (closed.compareAndSet(false, true)) {
            OPEN_STREAMS.remove(this);
          }
        }
      }
    };
    OPEN_STREAMS.add(tracked);
    return tracked;
  }
}
