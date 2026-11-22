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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileContext {
  private static final Logger LOG = LoggerFactory.getLogger(TestFileContext
      .class);

  private static final URI MOCK_FS_URI = URI.create("hdfs://nn:8020");
  private static final Path MOCK_FS_WORKING_DIR =
      new Path("hdfs://nn:8020/user/test");

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
    assertEquals("022", defaultlUMask, "Default UMask changed!");

    URI uri1 = new URI("file://mydfs:50070/");
    URI uri2 = new URI("file://tmp");

    FileContext fc1 = FileContext.getFileContext(uri1, conf);
    FileContext fc2 = FileContext.getFileContext(uri2, conf);
    assertEquals(022, fc1.getUMask().toShort(), "Umask for fc1 is incorrect");
    assertEquals(022, fc2.getUMask().toShort(), "Umask for fc2 is incorrect");

    // Till a user explicitly calls FileContext.setUMask(), the updates through
    // configuration should be reflected..
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "011");
    assertEquals(011, fc1.getUMask().toShort(), "Umask for fc1 is incorrect");
    assertEquals(011, fc2.getUMask().toShort(), "Umask for fc2 is incorrect");

    // Stop reflecting the conf update for specific FileContexts, once an
    // explicit setUMask is done.
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "066");
    fc1.setUMask(FsPermission.createImmutable((short) 00033));
    assertEquals(033, fc1.getUMask().toShort(), "Umask for fc1 is incorrect");
    assertEquals(066, fc2.getUMask().toShort(), "Umask for fc2 is incorrect");

    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    fc2.setUMask(FsPermission.createImmutable((short) 00044));
    assertEquals(033, fc1.getUMask().toShort(), "Umask for fc1 is incorrect");
    assertEquals(044, fc2.getUMask().toShort(), "Umask for fc2 is incorrect");
  }

  /**
   * The storage policy operations must pass the file system the path
   * qualified against the working directory, not the caller's relative path.
   */
  @Test
  public void testStoragePolicyOpsQualifyRelativePath() throws Exception {
    AbstractFileSystem afs = mockFileSystem();
    FileContext fc = FileContext.getFileContext(afs, new Configuration(false));
    Path relative = new Path("dir");
    Path qualified = new Path(MOCK_FS_WORKING_DIR, "dir");

    fc.setStoragePolicy(relative, "COLD");
    fc.unsetStoragePolicy(relative);
    fc.satisfyStoragePolicy(relative);

    verify(afs).setStoragePolicy(qualified, "COLD");
    verify(afs).unsetStoragePolicy(qualified);
    verify(afs).satisfyStoragePolicy(qualified);
  }

  /**
   * The storage policy operations must be applied to the target of a
   * symlink rather than retried on the link.
   */
  @Test
  public void testStoragePolicyOpsFollowSymlink() throws Exception {
    FileSystem.enableSymlinks();
    AbstractFileSystem afs = mockFileSystem();
    Path link = new Path(MOCK_FS_URI.toString(), "/link");
    Path target = new Path(MOCK_FS_URI.toString(), "/target");
    when(afs.getLinkTarget(any(Path.class))).thenReturn(target);
    doThrow(new UnresolvedLinkException()).when(afs)
        .setStoragePolicy(link, "COLD");
    doThrow(new UnresolvedLinkException()).when(afs).unsetStoragePolicy(link);
    doThrow(new UnresolvedLinkException()).when(afs).satisfyStoragePolicy(link);
    FileContext fc = FileContext.getFileContext(afs, new Configuration(false));

    fc.setStoragePolicy(link, "COLD");
    fc.unsetStoragePolicy(link);
    fc.satisfyStoragePolicy(link);

    verify(afs).setStoragePolicy(target, "COLD");
    verify(afs).unsetStoragePolicy(target);
    verify(afs).satisfyStoragePolicy(target);
  }

  private static AbstractFileSystem mockFileSystem() {
    AbstractFileSystem afs = mock(AbstractFileSystem.class);
    when(afs.getUri()).thenReturn(MOCK_FS_URI);
    when(afs.getInitialWorkingDirectory()).thenReturn(MOCK_FS_WORKING_DIR);
    return afs;
  }
}
