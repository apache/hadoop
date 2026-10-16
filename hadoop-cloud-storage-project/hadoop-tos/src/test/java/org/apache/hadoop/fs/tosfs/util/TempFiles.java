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

package org.apache.hadoop.fs.tosfs.util;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.Lists;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public final class TempFiles implements Closeable {
  private final List<String> files = Lists.newArrayList();
  private final List<String> dirs = Lists.newArrayList();

  private TempFiles() {
  }

  public static TempFiles of() {
    return new TempFiles();
  }

  public String newFile() {
    String p = newTempFile();
    files.add(p);
    return p;
  }

  public String newDir() {
    return newDir(null);
  }

  public String newDir(String prefix) {
    String p = newTempDir(prefix);
    dirs.add(p);
    return p;
  }

  @Override
  public void close() {
    files.forEach(file -> CommonUtils.runQuietly(() -> TempFiles.deleteFile(file)));
    files.clear();
    dirs.forEach(dir -> CommonUtils.runQuietly(() -> TempFiles.deleteDir(dir)));
    dirs.clear();
  }

  public static String newTempFile() {
    return String.join(File.pathSeparator, newTempDir(), UUIDUtils.random());
  }

  public static String newTempDir() {
    return newTempDir(null);
  }

  public static String newTempDir(String prefix) {
    try {
      return Files.createTempDirectory(prefix).toFile().getAbsolutePath();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void deleteFile(String path) {
    try {
      Files.deleteIfExists(Paths.get(path));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static void deleteDir(String path) {
    try {
      FileUtils.deleteDirectory(new File(path));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }
}
