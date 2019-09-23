/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.utils;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Utilities for zipping directories and adding existing directories to zips.
 */
public final class ZipUtilities {
  private ZipUtilities() {
    throw new UnsupportedOperationException("This class should not be " +
        "instantiated!");
  }

  private static final Logger LOG = LoggerFactory.getLogger(ZipUtilities.class);

  @VisibleForTesting
  public static String zipDir(String srcDir, String dstFile)
      throws IOException {
    FileOutputStream fos = new FileOutputStream(dstFile);
    ZipOutputStream zos = new ZipOutputStream(fos);
    File srcFile = new File(srcDir);
    LOG.info("Compressing directory {}", srcDir);
    addDirToZip(zos, srcFile, srcFile);
    // close the ZipOutputStream
    zos.close();
    LOG.info("Compressed directory {} to file: {}", srcDir, dstFile);
    return dstFile;
  }

  private static void addDirToZip(ZipOutputStream zos, File srcFile, File base)
      throws IOException {
    File[] files = srcFile.listFiles();
    if (files == null) {
      return;
    }
    for (File file : files) {
      // if it's directory, add recursively
      if (file.isDirectory()) {
        addDirToZip(zos, file, base);
        continue;
      }
      byte[] buffer = new byte[1024];
      try(FileInputStream fis = new FileInputStream(file)) {
        String name = base.toURI().relativize(file.toURI()).getPath();
        LOG.info("Adding file {} to zip", name);
        zos.putNextEntry(new ZipEntry(name));
        int length;
        while ((length = fis.read(buffer)) > 0) {
          zos.write(buffer, 0, length);
        }
        zos.flush();
      } finally {
        zos.closeEntry();
      }
    }
  }
}
