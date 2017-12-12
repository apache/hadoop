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

package org.apache.hadoop.mapred.nativetask;

import java.io.IOException;

import com.google.common.base.Charsets;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Task.TaskReporter;
import org.apache.hadoop.mapred.nativetask.util.ConfigUtil;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class stands for the native runtime It has three functions:
 * 1. Create native handlers for map, reduce, outputcollector, etc
 * 2. Configure native task with provided MR configs
 * 3. Provide file system api to native space, so that it can use File system like HDFS.
 */
@InterfaceAudience.Private
public class NativeRuntime {
  private static final Logger LOG =
      LoggerFactory.getLogger(NativeRuntime.class);
  private static boolean nativeLibraryLoaded = false;

  private static Configuration conf = new Configuration();

  static {
    try {
      System.loadLibrary("nativetask");
      LOG.info("Nativetask JNI library loaded.");
      nativeLibraryLoaded = true;
    } catch (final Throwable t) {
      // Ignore failures
      LOG.error("Failed to load nativetask JNI library with error: " + t);
      LOG.info("java.library.path=" + System.getProperty("java.library.path"));
      LOG.info("LD_LIBRARY_PATH=" + System.getenv("LD_LIBRARY_PATH"));
    }
  }

  private static void assertNativeLibraryLoaded() {
    if (!nativeLibraryLoaded) {
      throw new RuntimeException("Native runtime library not loaded");
    }
  }

  public static boolean isNativeLibraryLoaded() {
    return nativeLibraryLoaded;
  }

  public static void configure(Configuration jobConf) {
    assertNativeLibraryLoaded();
    conf = new Configuration(jobConf);
    conf.set(Constants.NATIVE_HADOOP_VERSION, VersionInfo.getVersion());
    JNIConfigure(ConfigUtil.toBytes(conf));
  }

  /**
   * create native object We use it to create native handlers
   */
  public synchronized static long createNativeObject(String clazz) {
    assertNativeLibraryLoaded();
    final long ret = JNICreateNativeObject(clazz.getBytes(Charsets.UTF_8));
    if (ret == 0) {
      LOG.warn("Can't create NativeObject for class " + clazz + ", probably not exist.");
    }
    return ret;
  }

  /**
   * Register a customized library
   */
  public synchronized static long registerLibrary(String libraryName, String clazz) {
    assertNativeLibraryLoaded();
    final long ret = JNIRegisterModule(libraryName.getBytes(Charsets.UTF_8),
                                       clazz.getBytes(Charsets.UTF_8));
    if (ret != 0) {
      LOG.warn("Can't create NativeObject for class " + clazz + ", probably not exist.");
    }
    return ret;
  }

  /**
   * destroy native object We use to destroy native handlers
   */
  public synchronized static void releaseNativeObject(long addr) {
    assertNativeLibraryLoaded();
    JNIReleaseNativeObject(addr);
  }

  /**
   * Get the status report from native space
   */
  public static void reportStatus(TaskReporter reporter) throws IOException {
    assertNativeLibraryLoaded();
    synchronized (reporter) {
      final byte[] statusBytes = JNIUpdateStatus();
      final DataInputBuffer ib = new DataInputBuffer();
      ib.reset(statusBytes, statusBytes.length);
      final FloatWritable progress = new FloatWritable();
      progress.readFields(ib);
      reporter.setProgress(progress.get());
      final Text status = new Text();
      status.readFields(ib);
      if (status.getLength() > 0) {
        reporter.setStatus(status.toString());
      }
      final IntWritable numCounters = new IntWritable();
      numCounters.readFields(ib);
      if (numCounters.get() == 0) {
        return;
      }
      final Text group = new Text();
      final Text name = new Text();
      final LongWritable amount = new LongWritable();
      for (int i = 0; i < numCounters.get(); i++) {
        group.readFields(ib);
        name.readFields(ib);
        amount.readFields(ib);
        reporter.incrCounter(group.toString(), name.toString(), amount.get());
      }
    }
  }


  /*******************************************************
   *** The following are JNI Apis
   ********************************************************/

  /**
   * Check whether the native side has compression codec support built in
   */
  public native static boolean supportsCompressionCodec(byte[] codec);

  /**
   * Config the native runtime with mapreduce job configurations.
   */
  private native static void JNIConfigure(byte[][] configs);

  /**
   * create a native object in native space
   */
  private native static long JNICreateNativeObject(byte[] clazz);

  /**
   * create the default native object for certain type
   */
  @Deprecated
  private native static long JNICreateDefaultNativeObject(byte[] type);

  /**
   * destroy native object in native space
   */
  private native static void JNIReleaseNativeObject(long addr);

  /**
   * Get status update from native side
   * Encoding:
   *  progress:float
   *  status:Text
   *  number: int the count of the counters
   *  Counters: array [group:Text, name:Text, incrCount:Long]
   */
  private native static byte[] JNIUpdateStatus();

  /**
   * Not used.
   */
  private native static void JNIRelease();

  /**
   * Not used.
   */
  private native static int JNIRegisterModule(byte[] path, byte[] name);
}
