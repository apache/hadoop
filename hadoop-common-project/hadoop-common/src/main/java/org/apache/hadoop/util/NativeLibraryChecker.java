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

package org.apache.hadoop.util;

import org.apache.hadoop.util.NativeCodeLoader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativeLibraryChecker {
  /**
   * A tool to test native library availability, 
   */
  public static void main(String[] args) {
    String usage = "NativeLibraryChecker [-a|-h]\n"
        + "  -a  use -a to check all libraries are available\n"
        + "      by default just check hadoop library is available\n"
        + "      exit with error code if check failed\n"
        + "  -h  print this message\n";
    if (args.length > 1 ||
        (args.length == 1 &&
            !(args[0].equals("-a") || args[0].equals("-h")))) {
      System.err.println(usage);
      ExitUtil.terminate(1);
    }
    boolean checkAll = false;
    if (args.length == 1) {
      if (args[0].equals("-h")) {
        System.out.println(usage);
        return;
      }
      checkAll = true;
    }
    boolean nativeHadoopLoaded = NativeCodeLoader.isNativeCodeLoaded();
    boolean zlibLoaded = false;
    boolean snappyLoaded = false;
    // lz4 is linked within libhadoop
    boolean lz4Loaded = nativeHadoopLoaded;
    if (nativeHadoopLoaded) {
      zlibLoaded = ZlibFactory.isNativeZlibLoaded(new Configuration());
      snappyLoaded = NativeCodeLoader.buildSupportsSnappy() &&
          SnappyCodec.isNativeCodeLoaded();
    }
    System.out.println("Native library checking:");
    System.out.printf("hadoop: %b\n", nativeHadoopLoaded);
    System.out.printf("zlib:   %b\n", zlibLoaded);
    System.out.printf("snappy: %b\n", snappyLoaded);
    System.out.printf("lz4:    %b\n", lz4Loaded);
    if ((!nativeHadoopLoaded) ||
        (checkAll && !(zlibLoaded && snappyLoaded && lz4Loaded))) {
      // return 1 to indicated check failed
      ExitUtil.terminate(1);
    }
  }
}
