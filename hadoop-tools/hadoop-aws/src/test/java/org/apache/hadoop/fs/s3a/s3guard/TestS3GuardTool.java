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

package org.apache.hadoop.fs.s3a.s3guard;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.Diff;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

import static org.apache.hadoop.fs.s3a.s3guard.S3GuardTool.SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test S3Guard related CLI commands against a LocalMetadataStore.
 */
public class TestS3GuardTool extends S3GuardToolTestBase {

  @Override
  protected MetadataStore newMetadataStore() {
    return new LocalMetadataStore();
  }

  @Test
  public void testImportCommand() throws IOException {
    S3AFileSystem fs = getFs();
    MetadataStore ms = getMetadataStore();
    Path parent = path("test-import");
    fs.mkdirs(parent);
    Path dir = new Path(parent, "a");
    fs.mkdirs(dir);
    Path emptyDir = new Path(parent, "emptyDir");
    fs.mkdirs(emptyDir);
    for (int i = 0; i < 10; i++) {
      String child = String.format("file-%d", i);
      try (FSDataOutputStream out = fs.create(new Path(dir, child))) {
        out.write(1);
      }
    }

    S3GuardTool.Import cmd = new S3GuardTool.Import(fs.getConf());
    cmd.setMetadataStore(ms);

    assertEquals("Import command did not exit successfully - see output",
        SUCCESS, cmd.run(new String[]{"import", parent.toString()}));

    DirListingMetadata children =
        ms.listChildren(dir);
    assertEquals("Unexpected number of paths imported", 10, children
        .getListing().size());
    assertEquals("Expected 2 items: empty directory and a parent directory", 2,
        ms.listChildren(parent).getListing().size());
    // assertTrue(children.isAuthoritative());
  }

  @Test
  public void testDiffCommand() throws IOException {
    S3AFileSystem fs = getFs();
    MetadataStore ms = getMetadataStore();
    Set<Path> filesOnS3 = new HashSet<>(); // files on S3.
    Set<Path> filesOnMS = new HashSet<>(); // files on metadata store.

    Path testPath = path("test-diff");
    mkdirs(testPath, true, true);

    Path msOnlyPath = new Path(testPath, "ms_only");
    mkdirs(msOnlyPath, false, true);
    filesOnMS.add(msOnlyPath);
    for (int i = 0; i < 5; i++) {
      Path file = new Path(msOnlyPath, String.format("file-%d", i));
      createFile(file, false, true);
      filesOnMS.add(file);
    }

    Path s3OnlyPath = new Path(testPath, "s3_only");
    mkdirs(s3OnlyPath, true, false);
    filesOnS3.add(s3OnlyPath);
    for (int i = 0; i < 5; i++) {
      Path file = new Path(s3OnlyPath, String.format("file-%d", i));
      createFile(file, true, false);
      filesOnS3.add(file);
    }

    ByteArrayOutputStream buf = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(buf);
    Diff cmd = new Diff(fs.getConf());
    cmd.setMetadataStore(ms);
    assertEquals("Diff command did not exit successfully - see output", SUCCESS,
        cmd.run(new String[]{"diff", "-meta", "local://metadata",
            testPath.toString()}, out));

    Set<Path> actualOnS3 = new HashSet<>();
    Set<Path> actualOnMS = new HashSet<>();
    boolean duplicates = false;
    try (ByteArrayInputStream in =
             new ByteArrayInputStream(buf.toByteArray())) {
      try (BufferedReader reader =
               new BufferedReader(new InputStreamReader(in))) {
        String line;
        while ((line = reader.readLine()) != null) {
          String[] fields = line.split("\\s");
          assertEquals("[" + line + "] does not have enough fields",
              4, fields.length);
          String where = fields[0];
          Path path = new Path(fields[3]);
          if (Diff.S3_PREFIX.equals(where)) {
            duplicates = duplicates || actualOnS3.contains(path);
            actualOnS3.add(path);
          } else if (Diff.MS_PREFIX.equals(where)) {
            duplicates = duplicates || actualOnMS.contains(path);
            actualOnMS.add(path);
          } else {
            fail("Unknown prefix: " + where);
          }
        }
      }
    }
    String actualOut = out.toString();
    assertEquals("Mismatched metadata store outputs: " + actualOut,
        filesOnMS, actualOnMS);
    assertEquals("Mismatched s3 outputs: " + actualOut, filesOnS3, actualOnS3);
    assertFalse("Diff contained duplicates", duplicates);
  }
}
