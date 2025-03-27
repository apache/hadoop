/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs.compat.common;


import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Random;

public final class HdfsCompatUtil {
  private static final Logger LOG =
      LoggerFactory.getLogger(HdfsCompatUtil.class);
  private static final Random RANDOM = new Random();

  private HdfsCompatUtil() {
  }

  public static void checkImplementation(ImplementationFunction func) {
    try {
      func.apply();
    } catch (UnsupportedOperationException e) {
      throw e;
    } catch (NoSuchMethodError e) {
      if (HdfsCompatApiScope.SKIP_NO_SUCH_METHOD_ERROR) {
        throw e;
      } else {
        throw new UnsupportedOperationException(e);
      }
    } catch (Throwable ignored) {
    }
  }

  public static void createFile(FileSystem fs, Path file, long fileLen)
      throws IOException {
    createFile(fs, file, true, 1024, fileLen, 1048576L, (short) 1);
  }

  public static void createFile(FileSystem fs, Path file, byte[] data)
      throws IOException {
    createFile(fs, file, true, data, 1048576L, (short) 1);
  }

  public static void createFile(FileSystem fs, Path file, boolean overwrite,
                                int bufferSize, long fileLen, long blockSize,
                                short replication) throws IOException {
    assert (bufferSize > 0);
    try (FSDataOutputStream out = fs.create(file, overwrite,
        bufferSize, replication, blockSize)) {
      if (fileLen > 0) {
        byte[] toWrite = new byte[bufferSize];
        long bytesToWrite = fileLen;
        while (bytesToWrite > 0) {
          RANDOM.nextBytes(toWrite);
          int bytesToWriteNext = (bufferSize < bytesToWrite) ?
              bufferSize : (int) bytesToWrite;
          out.write(toWrite, 0, bytesToWriteNext);
          bytesToWrite -= bytesToWriteNext;
        }
      }
    }
  }

  public static void createFile(FileSystem fs, Path file, boolean overwrite,
                                byte[] data, long blockSize,
                                short replication) throws IOException {
    try (FSDataOutputStream out = fs.create(file, overwrite,
        (data.length > 0) ? data.length : 1024, replication, blockSize)) {
      if (data.length > 0) {
        out.write(data);
      }
    }
  }

  public static byte[] readFileBuffer(FileSystem fs, Path fileName)
      throws IOException {
    try (ByteArrayOutputStream os = new ByteArrayOutputStream();
         FSDataInputStream in = fs.open(fileName)) {
      IOUtils.copyBytes(in, os, 1024, true);
      return os.toByteArray();
    }
  }

  public static void deleteQuietly(FileSystem fs, Path path,
                                   boolean recursive) {
    if (fs != null && path != null) {
      try {
        fs.delete(path, recursive);
      } catch (Throwable e) {
        LOG.warn("When deleting {}", path, e);
      }
    }
  }

  public interface ImplementationFunction {
    void apply() throws Exception;
  }
}