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
package org.apache.hadoop.fs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileContext {
  private static final Logger LOG = LoggerFactory.getLogger(TestFileContext
      .class);

  @Test
  public void testDefaultURIWithoutScheme() throws Exception {
    final Configuration conf = new Configuration();
    conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "/");
    try {
      FileContext.getFileContext(conf);
      fail(UnsupportedFileSystemException.class + " not thrown!");
    } catch (UnsupportedFileSystemException ufse) {
      LOG.info("Expected exception: ", ufse);
    }
  }

  @Test
  public void testConfBasedAndAPIBasedSetUMask() throws Exception {

    Configuration conf = new Configuration();

    String defaultlUMask =
        conf.get(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY);
    assertEquals("Default UMask changed!", "022", defaultlUMask);

    URI uri1 = new URI("file://mydfs:50070/");
    URI uri2 = new URI("file://tmp");

    FileContext fc1 = FileContext.getFileContext(uri1, conf);
    FileContext fc2 = FileContext.getFileContext(uri2, conf);
    assertEquals("Umask for fc1 is incorrect", 022, fc1.getUMask().toShort());
    assertEquals("Umask for fc2 is incorrect", 022, fc2.getUMask().toShort());

    // Till a user explicitly calls FileContext.setUMask(), the updates through
    // configuration should be reflected..
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "011");
    assertEquals("Umask for fc1 is incorrect", 011, fc1.getUMask().toShort());
    assertEquals("Umask for fc2 is incorrect", 011, fc2.getUMask().toShort());

    // Stop reflecting the conf update for specific FileContexts, once an
    // explicit setUMask is done.
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "066");
    fc1.setUMask(FsPermission.createImmutable((short) 00033));
    assertEquals("Umask for fc1 is incorrect", 033, fc1.getUMask().toShort());
    assertEquals("Umask for fc2 is incorrect", 066, fc2.getUMask().toShort());

    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    fc2.setUMask(FsPermission.createImmutable((short) 00044));
    assertEquals("Umask for fc1 is incorrect", 033, fc1.getUMask().toShort());
    assertEquals("Umask for fc2 is incorrect", 044, fc2.getUMask().toShort());
  }
}
