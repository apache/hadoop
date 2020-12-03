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

package org.apache.hadoop.runc.squashfs;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.runc.squashfs.inode.DirectoryINode;
import org.apache.hadoop.runc.squashfs.inode.FileINode;
import org.apache.hadoop.runc.squashfs.inode.INode;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestSquashFsConverter {

  @Rule
  public TemporaryFolder temp = new TemporaryFolder();

  private File tgz;

  @Before
  public void setUp() throws IOException {
    // create a tar.gz to import from
    tgz = temp.newFile("test.tar.gz");
    try (FileOutputStream fos = new FileOutputStream(tgz);
        GZIPOutputStream gos = new GZIPOutputStream(fos);
        TarArchiveOutputStream tos = new TarArchiveOutputStream(gos)) {

      // add a directory
      TarArchiveEntry dir = new TarArchiveEntry("dir/");
      dir.setMode((short) 0755);
      dir.setSize(0L);
      tos.putArchiveEntry(dir);
      tos.closeArchiveEntry();

      // add a file
      TarArchiveEntry file = new TarArchiveEntry("dir/file");
      file.setMode((short) 0644);
      file.setSize(4);
      tos.putArchiveEntry(file);
      tos.write("test".getBytes(StandardCharsets.UTF_8));
      tos.closeArchiveEntry();
    }
  }

  @Test
  public void simpleArchiveShouldConvertSuccessfully() throws IOException {
    File sqsh = temp.newFile("test.sqsh");

    SquashFsConverter.convertToSquashFs(tgz, sqsh);

    try (SquashFsReader reader = SquashFsReader.fromFile(sqsh)) {
      INode dir = reader.findInodeByPath("/dir");
      assertTrue("Dir is not a directory: " + dir.getClass().getName(),
          dir instanceof DirectoryINode);

      INode file = reader.findInodeByPath("/dir/file");
      assertTrue("File is not a file: " + file.getClass().getName(),
          file instanceof FileINode);

      FileINode fInode = (FileINode) file;

      assertEquals("Wrong file length", 4, fInode.getFileSize());

      byte[] buf = new byte[4];
      reader.read(file, 0L, buf, 0, 4);
      assertEquals("Wrong file data", "test",
          new String(buf, StandardCharsets.UTF_8));
    }
  }

}
