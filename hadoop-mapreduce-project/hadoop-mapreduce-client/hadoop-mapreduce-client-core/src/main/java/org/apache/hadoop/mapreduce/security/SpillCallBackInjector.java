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
package org.apache.hadoop.mapreduce.security;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Used for injecting callbacks while spilling files.
 * Calls into this are a no-op in production code.
 */
@VisibleForTesting
@InterfaceAudience.Private
public class SpillCallBackInjector {
  private static SpillCallBackInjector instance = new SpillCallBackInjector();
  public static SpillCallBackInjector get() {
    return instance;
  }
  /**
   * Sets the global SpillFilesCBInjector to the new value, returning the old
   * value.
   *
   * @param spillInjector the new implementation for the spill injector.
   * @return the previous implementation.
   */
  public static SpillCallBackInjector getAndSet(
      SpillCallBackInjector spillInjector) {
    SpillCallBackInjector prev = instance;
    instance = spillInjector;
    return prev;
  }

  public void writeSpillIndexFileCB(Path path) {
    // do nothing
  }

  public void writeSpillFileCB(Path path, FSDataOutputStream out,
      Configuration conf) {
    // do nothing
  }

  public void getSpillFileCB(Path path, InputStream is, Configuration conf) {
    // do nothing
  }

  public String getSpilledFileReport() {
    return null;
  }

  public void handleErrorInSpillFill(Path path, Exception e) {
    // do nothing
  }

  public void corruptSpilledFile(Path fileName) throws IOException {
    // do nothing
  }

  public void addSpillIndexFileCB(Path path, Configuration conf) {
    // do nothing
  }

  public void validateSpillIndexFileCB(Path path, Configuration conf) {
    // do nothing
  }
}
