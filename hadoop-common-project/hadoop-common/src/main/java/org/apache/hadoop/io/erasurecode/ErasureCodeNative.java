/*
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

package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.util.NativeCodeLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Erasure code native libraries (for now, Intel ISA-L) related utilities.
 */
public final class ErasureCodeNative {

  private static final Logger LOG =
      LoggerFactory.getLogger(ErasureCodeNative.class.getName());

  /**
   * The reason why ISA-L library is not available, or null if it is available.
   */
  private static final String LOADING_FAILURE_REASON;

  static {
    if (!NativeCodeLoader.isNativeCodeLoaded()) {
      LOADING_FAILURE_REASON = "hadoop native library cannot be loaded.";
    } else if (!NativeCodeLoader.buildSupportsIsal()) {
      LOADING_FAILURE_REASON = "libhadoop was built without ISA-L support";
    } else {
      String problem = null;
      try {
        loadLibrary();
      } catch (Throwable t) {
        problem = "Loading ISA-L failed: " + t.getMessage();
        LOG.warn(problem);
      }
      LOADING_FAILURE_REASON = problem;
    }

    if (LOADING_FAILURE_REASON != null) {
      LOG.warn("ISA-L support is not available in your platform... " +
              "using builtin-java codec where applicable");
    }
  }

  private ErasureCodeNative() {}

  /**
   * Are native libraries loaded?
   */
  public static boolean isNativeCodeLoaded() {
    return LOADING_FAILURE_REASON == null;
  }

  /**
   * Is the native ISA-L library loaded and initialized? Throw exception if not.
   */
  public static void checkNativeCodeLoaded() {
    if (LOADING_FAILURE_REASON != null) {
      throw new RuntimeException(LOADING_FAILURE_REASON);
    }
  }

  /**
   * Load native library available or supported.
   */
  public static native void loadLibrary();

  /**
   * Get the native library name that's available or supported.
   */
  public static native String getLibraryName();

  public static String getLoadingFailureReason() {
    return LOADING_FAILURE_REASON;
  }
}
