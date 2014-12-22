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
import org.apache.hadoop.crypto.OpensslCipher;
import org.apache.hadoop.io.compress.Lz4Codec;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
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
        + "      by default just check hadoop library (and\n"
        + "      winutils.exe on Windows OS) is available\n"
        + "      exit with error code 1 if check failed\n"
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
    Configuration conf = new Configuration();
    boolean nativeHadoopLoaded = NativeCodeLoader.isNativeCodeLoaded();
    boolean zlibLoaded = false;
    boolean snappyLoaded = false;
    // lz4 is linked within libhadoop
    boolean lz4Loaded = nativeHadoopLoaded;
    boolean bzip2Loaded = Bzip2Factory.isNativeBzip2Loaded(conf);
    boolean openSslLoaded = false;
    boolean winutilsExists = false;

    String openSslDetail = "";
    String hadoopLibraryName = "";
    String zlibLibraryName = "";
    String snappyLibraryName = "";
    String lz4LibraryName = "";
    String bzip2LibraryName = "";
    String winutilsPath = null;

    if (nativeHadoopLoaded) {
      hadoopLibraryName = NativeCodeLoader.getLibraryName();
      zlibLoaded = ZlibFactory.isNativeZlibLoaded(conf);
      if (zlibLoaded) {
        zlibLibraryName = ZlibFactory.getLibraryName();
      }
      snappyLoaded = NativeCodeLoader.buildSupportsSnappy() &&
          SnappyCodec.isNativeCodeLoaded();
      if (snappyLoaded && NativeCodeLoader.buildSupportsSnappy()) {
        snappyLibraryName = SnappyCodec.getLibraryName();
      }
      if (OpensslCipher.getLoadingFailureReason() != null) {
        openSslDetail = OpensslCipher.getLoadingFailureReason();
        openSslLoaded = false;
      } else {
        openSslDetail = OpensslCipher.getLibraryName();
        openSslLoaded = true;
      }
      if (lz4Loaded) {
        lz4LibraryName = Lz4Codec.getLibraryName();
      }
      if (bzip2Loaded) {
        bzip2LibraryName = Bzip2Factory.getLibraryName(conf);
      }
    }

    // winutils.exe is required on Windows
    winutilsPath = Shell.getWinUtilsPath();
    if (winutilsPath != null) {
      winutilsExists = true;
    } else {
      winutilsPath = "";
    }

    System.out.println("Native library checking:");
    System.out.printf("hadoop:  %b %s%n", nativeHadoopLoaded, hadoopLibraryName);
    System.out.printf("zlib:    %b %s%n", zlibLoaded, zlibLibraryName);
    System.out.printf("snappy:  %b %s%n", snappyLoaded, snappyLibraryName);
    System.out.printf("lz4:     %b %s%n", lz4Loaded, lz4LibraryName);
    System.out.printf("bzip2:   %b %s%n", bzip2Loaded, bzip2LibraryName);
    System.out.printf("openssl: %b %s%n", openSslLoaded, openSslDetail);
    if (Shell.WINDOWS) {
      System.out.printf("winutils: %b %s%n", winutilsExists, winutilsPath);
    }

    if ((!nativeHadoopLoaded) || (Shell.WINDOWS && (!winutilsExists)) ||
        (checkAll && !(zlibLoaded && snappyLoaded && lz4Loaded && bzip2Loaded))) {
      // return 1 to indicated check failed
      ExitUtil.terminate(1);
    }
  }
}
