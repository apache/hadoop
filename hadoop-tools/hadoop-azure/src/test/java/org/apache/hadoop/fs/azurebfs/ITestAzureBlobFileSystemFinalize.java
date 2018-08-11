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

package org.apache.hadoop.fs.azurebfs;

import java.lang.ref.WeakReference;

import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * Test finalize() method when "fs.abfs.impl.disable.cache" is enabled.
 */
public class ITestAzureBlobFileSystemFinalize extends AbstractAbfsScaleTest{
  static final String DISABLE_CACHE_KEY = "fs.abfs.impl.disable.cache";

  public ITestAzureBlobFileSystemFinalize() throws Exception {
    super();
  }

  @Test
  public void testFinalize() throws Exception {
    // Disable the cache for filesystem to make sure there is no reference.
    Configuration configuration = this.getConfiguration();
    configuration.setBoolean(this.DISABLE_CACHE_KEY, true);

    AzureBlobFileSystem fs = (AzureBlobFileSystem) FileSystem.get(configuration);

    WeakReference<Object> ref = new WeakReference<Object>(fs);
    fs = null;

    int i = 0;
    int maxTries = 1000;
    while (ref.get() != null && i < maxTries) {
      System.gc();
      System.runFinalization();
      i++;
    }

    Assert.assertTrue("testFinalizer didn't get cleaned up within maxTries", ref.get() == null);
  }
}