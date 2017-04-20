/*
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

package org.apache.slider.other;

import org.apache.slider.utils.SliderTestUtils;
import org.apache.slider.utils.TestUtility;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * This test exists to diagnose local FS permissions.
 */
public class TestLocalDirStatus extends SliderTestUtils {


  public static final int SIZE = 0x200000;

  @Test
  public void testTempDir() throws Throwable {
    File tmpf = null;
    try {
      tmpf = File.createTempFile("testl", ".bin");
      createAndReadFile(tmpf, SIZE);
      tmpf.delete();
      assertFalse(tmpf.exists());
    } finally {
      if (tmpf != null) {
        tmpf.delete();
      }
    }
  }

  @Test
  public void testTargetDir() throws Throwable {
    File target = target();
    File tmpf = null;
    try {
      tmpf = File.createTempFile("testl", ".bin", target);
      createAndReadFile(tmpf, SIZE);
      tmpf.delete();
      assertFalse(tmpf.exists());
    } finally {
      if (tmpf != null) {
        tmpf.delete();
      }

    }
  }

  public File target() {
    File target = new File("target").getAbsoluteFile();
    assertTrue(target.exists());
    return target;
  }

  @Test
  public void testRenameInTargetDir() throws Throwable {
    File target = target();
    File tmpf = null;
    File dst= null;
    try {
      tmpf = File.createTempFile("testl", ".bin", target);
      dst = File.createTempFile("test-dest", ".bin", target);
      createRenameAndReadFile(tmpf, dst, SIZE);
      assertFalse(tmpf.exists());
      dst.delete();
    } finally {
      if (tmpf != null) {
        tmpf.delete();
      }
      if (dst != null) {
        dst.delete();
      }
    }
  }

  @Test
  public void testRenameInTmpDir() throws Throwable {
    File tmpf = null;
    File dst= null;
    try {
      tmpf = File.createTempFile("testl", ".bin");
      dst = File.createTempFile("test-dest", ".bin");
      createRenameAndReadFile(tmpf, dst, SIZE);
      assertFalse(tmpf.exists());
      dst.delete();
    } finally {
      if (tmpf != null) {
        tmpf.delete();
      }
      if (dst != null) {
        dst.delete();
      }
    }
  }

  protected void createAndReadFile(File path, int len) throws IOException {
    byte[] dataset = TestUtility.dataset(len, 32, 128);
    writeFile(path, dataset);
    assertTrue(path.exists());
    assertEquals(len, path.length());
    byte[] persisted = readFile(path);
    TestUtility.compareByteArrays(dataset, persisted, len);
  }

  protected void createRenameAndReadFile(File src, File dst, int len)
      throws IOException {
    byte[] dataset = TestUtility.dataset(len, 32, 128);
    writeFile(src, dataset);
    assertTrue(src.exists());
    assertEquals(len, src.length());
    dst.delete();
    assertFalse(dst.exists());
    assertTrue(src.renameTo(dst));
    assertEquals(len, dst.length());
    byte[] persisted = readFile(dst);
    TestUtility.compareByteArrays(dataset, persisted, len);
  }

  protected void writeFile(File path, byte[] dataset)
      throws IOException {
    FileOutputStream out = new FileOutputStream(path);
    try {
      out.write(dataset);
      out.flush();
    } finally {
      out.close();
    }
  }

  protected byte[] readFile(File path) throws IOException {
    assertTrue(path.getAbsoluteFile().exists());
    assertTrue(path.getAbsoluteFile().isFile());
    int len = (int)path.length();
    byte[] dataset = new byte[len];
    FileInputStream ins = new FileInputStream(path);
    try {
      ins.read(dataset);
    } finally {
      ins.close();
    }
    return dataset;
  }


}
