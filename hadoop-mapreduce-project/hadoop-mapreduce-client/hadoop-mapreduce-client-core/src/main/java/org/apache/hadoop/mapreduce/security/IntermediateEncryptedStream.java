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
package org.apache.hadoop.mapreduce.security;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.CryptoUtils;

/**
 * Used to wrap helpers while spilling intermediate files.
 * Setting the {@link SpillCallBackInjector} helps in:
 *   1- adding callbacks to capture the path of the spilled files.
 *   2- Verifying the encryption when intermediate encryption is enabled.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class IntermediateEncryptedStream {

  private static SpillCallBackInjector prevSpillCBInjector = null;

  public static FSDataOutputStream wrapIfNecessary(Configuration conf,
      FSDataOutputStream out, Path outPath) throws IOException {
    SpillCallBackInjector.get().writeSpillFileCB(outPath, out, conf);
    return CryptoUtils.wrapIfNecessary(conf, out, true);
  }

  public static FSDataOutputStream wrapIfNecessary(Configuration conf,
      FSDataOutputStream out, boolean closeOutputStream,
      Path outPath) throws IOException {
    SpillCallBackInjector.get().writeSpillFileCB(outPath, out, conf);
    return CryptoUtils.wrapIfNecessary(conf, out, closeOutputStream);
  }

  public static FSDataInputStream wrapIfNecessary(Configuration conf,
      FSDataInputStream in, Path inputPath) throws IOException {
    SpillCallBackInjector.get().getSpillFileCB(inputPath, in, conf);
    return CryptoUtils.wrapIfNecessary(conf, in);
  }

  public static InputStream wrapIfNecessary(Configuration conf,
      InputStream in, long length, Path inputPath) throws IOException {
    SpillCallBackInjector.get().getSpillFileCB(inputPath, in, conf);
    return CryptoUtils.wrapIfNecessary(conf, in, length);
  }

  public static void addSpillIndexFile(Path indexFilename, Configuration conf) {
    SpillCallBackInjector.get().addSpillIndexFileCB(indexFilename, conf);
  }

  public static void validateSpillIndexFile(Path indexFilename,
      Configuration conf) {
    SpillCallBackInjector.get().validateSpillIndexFileCB(indexFilename, conf);
  }

  public static SpillCallBackInjector resetSpillCBInjector() {
    return setSpillCBInjector(prevSpillCBInjector);
  }

  public synchronized static SpillCallBackInjector setSpillCBInjector(
      SpillCallBackInjector spillInjector) {
    prevSpillCBInjector =
        SpillCallBackInjector.getAndSet(spillInjector);
    return spillInjector;
  }

  private IntermediateEncryptedStream() {}
}
