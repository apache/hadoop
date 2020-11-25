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

import org.apache.hadoop.io.compress.ZStandardCodec;
import org.apache.hadoop.io.erasurecode.ErasureCodeNative;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.OpensslCipher;
import org.apache.hadoop.io.compress.bzip2.Bzip2Factory;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class NativeLibraryChecker {
  public static final Logger LOG =
      LoggerFactory.getLogger(NativeLibraryChecker.class);

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
    boolean isalLoaded = false;
    boolean zStdLoaded = false;
    boolean pmdkLoaded = false;
    boolean bzip2Loaded = Bzip2Factory.isNativeBzip2Loaded(conf);
    boolean openSslLoaded = false;
    boolean winutilsExists = false;

    String openSslDetail = "";
    String hadoopLibraryName = "";
    String zlibLibraryName = "";
    String isalDetail = "";
    String pmdkDetail = "";
    String zstdLibraryName = "";
    String bzip2LibraryName = "";
    String winutilsPath = null;

    if (nativeHadoopLoaded) {
      hadoopLibraryName = NativeCodeLoader.getLibraryName();
      zlibLoaded = ZlibFactory.isNativeZlibLoaded(conf);
      if (zlibLoaded) {
        zlibLibraryName = ZlibFactory.getLibraryName();
      }
      zStdLoaded = NativeCodeLoader.buildSupportsZstd() &&
        ZStandardCodec.isNativeCodeLoaded();
      if (zStdLoaded && NativeCodeLoader.buildSupportsZstd()) {
        zstdLibraryName = ZStandardCodec.getLibraryName();
      }

      isalDetail = ErasureCodeNative.getLoadingFailureReason();
      if (isalDetail != null) {
        isalLoaded = false;
      } else {
        isalDetail = ErasureCodeNative.getLibraryName();
        isalLoaded = true;
      }

      pmdkDetail = NativeIO.POSIX.getPmdkSupportStateMessage();
      pmdkLoaded = NativeIO.POSIX.isPmdkAvailable();
      if (pmdkLoaded) {
        pmdkDetail = NativeIO.POSIX.Pmem.getPmdkLibPath();
      }

      openSslDetail = OpensslCipher.getLoadingFailureReason();
      if (openSslDetail != null) {
        openSslLoaded = false;
      } else {
        openSslDetail = OpensslCipher.getLibraryName();
        openSslLoaded = true;
      }

      if (bzip2Loaded) {
        bzip2LibraryName = Bzip2Factory.getLibraryName(conf);
      }
    }

    if (Shell.WINDOWS) {
      // winutils.exe is required on Windows
      try {
        winutilsPath = Shell.getWinUtilsFile().getCanonicalPath();
        winutilsExists = true;
      } catch (IOException e) {
        LOG.debug("No Winutils: ", e);
        winutilsPath = e.getMessage();
        winutilsExists = false;
      }
      System.out.printf("winutils: %b %s%n", winutilsExists, winutilsPath);
    }

    System.out.println("Native library checking:");
    System.out.printf("hadoop:  %b %s%n", nativeHadoopLoaded, hadoopLibraryName);
    System.out.printf("zlib:    %b %s%n", zlibLoaded, zlibLibraryName);
    System.out.printf("zstd  :  %b %s%n", zStdLoaded, zstdLibraryName);
    System.out.printf("bzip2:   %b %s%n", bzip2Loaded, bzip2LibraryName);
    System.out.printf("openssl: %b %s%n", openSslLoaded, openSslDetail);
    System.out.printf("ISA-L:   %b %s%n", isalLoaded, isalDetail);
    System.out.printf("PMDK:    %b %s%n", pmdkLoaded, pmdkDetail);

    if (Shell.WINDOWS) {
      System.out.printf("winutils: %b %s%n", winutilsExists, winutilsPath);
    }

    if ((!nativeHadoopLoaded) || (Shell.WINDOWS && (!winutilsExists)) ||
        (checkAll && !(zlibLoaded && bzip2Loaded
            && isalLoaded && zStdLoaded))) {
      // return 1 to indicated check failed
      ExitUtil.terminate(1);
    }
  }
}
