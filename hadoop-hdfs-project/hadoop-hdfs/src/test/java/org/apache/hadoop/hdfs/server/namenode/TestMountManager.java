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
package org.apache.hadoop.hdfs.server.namenode;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.MountException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockManager;
import org.apache.hadoop.hdfs.server.blockmanagement.ProvidedStorageMap;
import org.apache.hadoop.hdfs.server.common.ProvidedVolumeInfo;
import org.apache.hadoop.test.LambdaTestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link MountManager}.
 */
public class TestMountManager {

  private Configuration conf;
  private MountManager mountManager;
  private String tempDir;
  private FSDirectory mockFSDir;

  @Before
  public void setup() {
    conf = new Configuration();
    tempDir = MountManager.MOUNT_TMP_DIR.substring(0,
        MountManager.MOUNT_TMP_DIR.length() - 1);
    FSNamesystem mockFSN = mock(FSNamesystem.class);
    mockFSDir = mock(FSDirectory.class);
    when(mockFSN.getFSDirectory()).thenReturn(mockFSDir);
    BlockManager mockBlockManager = mock(BlockManager.class);
    ProvidedStorageMap mockProvidedStorageMap = mock(ProvidedStorageMap.class);

    when(mockFSN.getBlockManager()).thenReturn(mockBlockManager);
    when(mockBlockManager.getProvidedStorageMap())
        .thenReturn(mockProvidedStorageMap);
    when(mockProvidedStorageMap.isProvidedEnabled()).thenReturn(true);

    mountManager = MountManager.createInstance(conf, mockFSN);
  }

  @Test
  public void testAddRemoveMountCycle() throws Exception {
    Path mount1 = new Path("/remotes/mounts/mount1");
    String remote1 = "hdfs://nn0:1010/shares/data/";
    ProvidedVolumeInfo volumeInfo = new ProvidedVolumeInfo(
        UUID.randomUUID(), mount1.toString(), remote1, new HashMap<>());

    // start the mount
    mountManager.startMount(mount1, volumeInfo);
    // start, remove on this path should throw an exception.
    LambdaTestUtils.intercept(IOException.class,
        MountException.mountAlreadyExistsException(mount1).getMessage(),
        () -> mountManager.startMount(mount1, new ProvidedVolumeInfo()));

    // ending with "/" is the same as not ending in "/"
    String mount1EndingSlash = mount1 + "/";
    LambdaTestUtils.intercept(IOException.class,
        MountException.mountAlreadyExistsException(new Path(mount1EndingSlash))
            .getMessage(),
        () -> mountManager.startMount(
            new Path(mount1EndingSlash), new ProvidedVolumeInfo()));

    Path mount2 = new Path(mount1, "extrapath");
    LambdaTestUtils.intercept(IOException.class,
        "Mount path " + mount2 + " is not being created",
        () -> mountManager.finishMount(mount2, false));

    // finish the mount
    mountManager.finishMount(mount1, false);

    // should contain one mount point
    assertEquals(1, mountManager.getMountPoints().size());
    assertEquals(volumeInfo,
        mountManager.getMountPoints().get(mount1));

    // check that getMountPath is correct!
    assertEquals(mount1, mountManager.getMountPath(mount1));

    assertEquals(mount1,
        mountManager.getMountPath(new Path(mount1, "extrapath")));

    assertNull(mountManager.getMountPath(new Path("/extrapath")));
    assertNull(mountManager.getMountPath(new Path(mount1 + "extrapath")));

    Path mount3 = new Path("/remotes/mounts/mount2/");

    mountManager.startMount(mount3, volumeInfo);
    // should contain two mount points now
    assertEquals(2, mountManager.getMountPoints().size());
    assertEquals(volumeInfo,
        mountManager.getMountPoints().get(mount3));
    mountManager.finishMount(mount3, false);

    when(mockFSDir.getINode(mount1.toString())).thenReturn(null);
    // remove the first mount
    mountManager.removeMountPoint(mount1);

    // should contain one mount points now
    assertEquals(1, mountManager.getMountPoints().size());
    assertEquals(volumeInfo,
        mountManager.getMountPoints().get(mount3));
    // not mapping should exist for the first mount
    assertNull(mountManager.getMountPoints().get(mount1));

    when(mockFSDir.getINode(mount3.toString())).thenReturn(null);
    // remove mount3
    mountManager.removeMountPoint(mount3);
    // should contain nothing now.
    assertEquals(0, mountManager.getMountPoints().size());

    // should be able to add mount1 again.
    mountManager.startMount(mount1, volumeInfo);
    mountManager.finishMount(mount1, false);
    assertEquals(1, mountManager.getMountPoints().size());
    assertEquals(volumeInfo,
        mountManager.getMountPoints().get(mount1));
  }

  @Test
  public void testTemporaryPaths() throws IOException {
    String rootTemporaryDir = mountManager.getRootTemporaryDir();
    assertEquals(tempDir, rootTemporaryDir);
    Path mount = new Path("/mount1");
    UUID uuid = UUID.randomUUID();
    String remotePath = "hdfs://nn1/data/";
    ProvidedVolumeInfo volumeInfo =
        new ProvidedVolumeInfo(uuid, mount.toString(),
            remotePath, Collections.emptyMap());
    String tempPath = mountManager.startMount(mount, volumeInfo);
    // temp path should start with the base temp path
    // end with the mount
    // and contain the uuid.
    assertEquals(tempDir + "/" + uuid + mount, tempPath);
    assertEquals(tempPath,
        mountManager.getTemporaryPathForMount(mount, volumeInfo));
    assertEquals(mount,
        mountManager.getMountFromTemporaryPath(tempPath));
    String subDir = new Path(tempPath, "path1").toString();
    assertEquals(mount,
        mountManager.getMountFromTemporaryPath(subDir));
  }

  @Test
  public void testTempDirectoryPattern() {
    Pattern tempDirPattern = mountManager.getMountTempDirectoryPattern();
    // the pattern should only match paths that being with tempDir
    assertFalse(tempDirPattern.matcher(tempDir).matches());
    assertFalse(tempDirPattern.matcher(tempDir + "/").matches());
    assertFalse(tempDirPattern.matcher(tempDir + "/uuid").matches());
    assertFalse(tempDirPattern.matcher(tempDir + "/uuid/").matches());
    Matcher matcher = tempDirPattern.matcher(tempDir + "/uuid/mount/path");
    assertTrue(matcher.matches());
    assertEquals(2, matcher.groupCount());
    assertEquals("uuid", matcher.group(1));
    assertEquals("/mount/path", matcher.group(2));
    // fails on a path that doesn't start with temp dir.
    assertFalse(tempDirPattern.matcher("/uuid/mount/path").matches());
  }
}
