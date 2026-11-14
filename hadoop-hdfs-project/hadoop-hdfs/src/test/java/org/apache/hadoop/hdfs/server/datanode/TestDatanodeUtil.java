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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.net.URI;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Tests for {@link DatanodeUtil}.
 */
public class TestDatanodeUtil {

  @TempDir
  private File tempDir;

  @Test
  public void testFileToURIMatchesFileToURIForRegularFile() throws Exception {
    File blockFile = new File(tempDir, "blk_1073741825");
    assertTrue(blockFile.createNewFile());
    File metaFile = new File(tempDir, "blk_1073741825_1001.meta");
    assertTrue(metaFile.createNewFile());

    assertEquals(blockFile.toURI(), DatanodeUtil.fileToURI(blockFile));
    assertEquals(metaFile.toURI(), DatanodeUtil.fileToURI(metaFile));
  }

  @Test
  public void testFileToURIMatchesFileToURIForMissingFile() {
    File missing = new File(tempDir, "blk_42");
    assertFalse(missing.exists());
    assertEquals(missing.toURI(), DatanodeUtil.fileToURI(missing));
  }

  @Test
  public void testFileToURIEscapesAndRoundTrips() throws Exception {
    File dir = new File(tempDir, "data dir#1%");
    assertTrue(dir.mkdir());
    File blockFile = new File(dir, "blk_7");
    assertTrue(blockFile.createNewFile());

    URI uri = DatanodeUtil.fileToURI(blockFile);
    assertEquals(blockFile.toURI(), uri);
    assertEquals(blockFile.getAbsoluteFile(), new File(uri));
  }

  @Test
  public void testFileToURIResolvesRelativePath() {
    File relative = new File("blk_9");
    assertEquals(relative.toURI(), DatanodeUtil.fileToURI(relative));
  }

  @Test
  public void testFileToURIDoesNotAppendSlashForDirectory() {
    URI uri = DatanodeUtil.fileToURI(tempDir);
    assertFalse(uri.getPath().endsWith("/"));
    assertEquals(tempDir.getAbsoluteFile(), new File(uri));
  }
}
