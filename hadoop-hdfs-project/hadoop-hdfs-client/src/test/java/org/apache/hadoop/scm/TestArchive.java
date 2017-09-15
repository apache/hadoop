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
package org.apache.hadoop.scm;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.hadoop.fs.FileUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Random;
import java.util.zip.Adler32;
import java.util.zip.Checksum;

/**
 * Test archive creation and unpacking.
 */
public class TestArchive {
  private static final int DIR_COUNT = 10;
  private static final int SUB_DIR_COUNT = 3;
  private static final int FILE_COUNT = 10;
  private long checksumWrite = 0L;
  private long checksumRead = 0L;
  private long tmp = 0L;
  private Checksum crc = new Adler32();

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public TemporaryFolder outputFolder = new TemporaryFolder();


  @Before
  public void setUp() throws Exception {
    Random r = new Random();
    final int megaByte = 1024 * 1024;

    for (int x = 0; x < DIR_COUNT; x++) {
      File subdir = folder.newFolder(String.format("dir%d", x));
      for (int y = 0; y < SUB_DIR_COUNT; y++) {
        File targetDir = new File(subdir.getPath().concat(File.separator)
            .concat(String.format("subdir%d%d", x, y)));
        if(!targetDir.mkdirs()) {
          throw new IOException("Failed to create subdirectory. " +
              targetDir.toString());
        }
        for (int z = 0; z < FILE_COUNT; z++) {
          Path temp = Paths.get(targetDir.getPath().concat(File.separator)
              .concat(String.format("File%d.txt", z)));
          byte[] buf = RandomUtils.nextBytes(r.nextInt(megaByte));
          Files.write(temp, buf);
          crc.reset();
          crc.update(buf, 0, buf.length);
          tmp = crc.getValue();
          checksumWrite +=tmp;
        }
      }
    }
  }

  @Test
  public void testArchive() throws Exception {
    File archiveFile = new File(outputFolder.getRoot() + File.separator
        + "test.container.zip");
    long zipCheckSum = FileUtil.zip(folder.getRoot(), archiveFile);
    Assert.assertTrue(zipCheckSum > 0);
    File decomp = new File(outputFolder.getRoot() + File.separator +
        "decompress");
    if (!decomp.exists() && !decomp.mkdirs()) {
      throw new IOException("Unable to create the destination directory. " +
          decomp.getPath());
    }

    FileUtil.unZip(archiveFile, decomp);
    String[] patterns = {"txt"};
    Iterator<File> iter = FileUtils.iterateFiles(decomp, patterns, true);
    int count = 0;
    while (iter.hasNext()) {
      count++;
      byte[] buf = Files.readAllBytes(iter.next().toPath());
      crc.reset();
      crc.update(buf, 0, buf.length);
      tmp = crc.getValue();
      checksumRead += tmp;
    }
    Assert.assertEquals(DIR_COUNT * SUB_DIR_COUNT * FILE_COUNT, count);
    Assert.assertEquals(checksumWrite, checksumRead);
  }
}
