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
package org.apache.hadoop.hdfs.server.datanode;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * Used for injecting call backs in {@link VolumeScanner}
 * and {@link BlockScanner} tests.
 * Calls into this are a no-op in production code.
 */
@VisibleForTesting
@InterfaceAudience.Private
public class VolumeScannerCBInjector {
  private static VolumeScannerCBInjector instance =
      new VolumeScannerCBInjector();

  public static VolumeScannerCBInjector get() {
    return instance;
  }

  public static void set(VolumeScannerCBInjector injector) {
    instance = injector;
  }

  public void preSavingBlockIteratorTask(final VolumeScanner volumeScanner) {
  }

  public void shutdownCallBack(final VolumeScanner volumeScanner) {
  }

  public void terminationCallBack(final VolumeScanner volumeScanner) {
  }
}
