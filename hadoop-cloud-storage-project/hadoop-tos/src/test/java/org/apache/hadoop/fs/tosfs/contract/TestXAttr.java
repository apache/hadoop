/*
 * ByteDance Volcengine EMR, Copyright 2022.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.tosfs.contract;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.XAttrSetFlag;
import org.apache.hadoop.fs.contract.AbstractFSContract;
import org.apache.hadoop.fs.contract.AbstractFSContractTestBase;
import org.apache.hadoop.fs.tosfs.TestEnv;
import org.apache.hadoop.fs.tosfs.common.Bytes;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

public class TestXAttr extends AbstractFSContractTestBase {
  private static final String XATTR_NAME = "xAttrName";
  private static final byte[] XATTR_VALUE = "xAttrValue".getBytes();

  @BeforeClass
  public static void before() {
    Assume.assumeTrue(TestEnv.checkTestEnabled());
  }

  @Override
  protected AbstractFSContract createContract(Configuration conf) {
    return new TosContract(conf);
  }

  @Test
  public void testGetNonExistedXAttr() throws Exception {
    FileSystem fs = getFileSystem();
    Path path = path("testSetAndGet/file");
    fs.create(path).close();

    fs.setXAttr(path, XATTR_NAME, XATTR_VALUE);
    assertThrows("Not found.", IOException.class,
        () -> fs.getXAttr(path, "non-exist"));
    assertThrows("Not found.", IOException.class,
        () -> fs.getXAttrs(path, Arrays.asList("non-exist")));
    assertThrows("Not found.", IOException.class,
        () -> fs.getXAttrs(path, Arrays.asList("non-exist", XATTR_NAME)));
  }

  @Test
  public void testSetAndGetWhenPathNotExist() throws Exception {
    FileSystem fs = getFileSystem();
    Path path = path("testXAttrWhenPathNotExist/file");
    fs.delete(path);

    assertThrows("No such file", FileNotFoundException.class,
        () -> fs.setXAttr(path, XATTR_NAME, XATTR_VALUE));
    assertThrows("No such file", FileNotFoundException.class,
        () -> fs.getXAttrs(path));
    assertThrows("No such file", FileNotFoundException.class,
        () -> fs.removeXAttr(path, "name"));
  }

  @Test
  public void testSetAndGet() throws Exception {
    FileSystem fs = getFileSystem();
    Path path = path("testSetAndGet/file");
    fs.create(path).close();

    fs.setXAttr(path, XATTR_NAME, XATTR_VALUE);
    assertArrayEquals(XATTR_VALUE, fs.getXAttr(path, XATTR_NAME));
  }

  @Test
  public void testSetAndGetNonExistedObject() throws Exception {
    FileSystem fs = getFileSystem();
    Path path = path("testSetAndGetOnNonExistedObject/dir-0/dir-1/file");
    fs.create(path).close();

    Path nonExistedPath = path.getParent().getParent();
    fs.setXAttr(nonExistedPath, XATTR_NAME, XATTR_VALUE);
    assertThrows("Not found.", IOException.class,
        () -> fs.getXAttr(nonExistedPath, XATTR_NAME));
  }

  @Test
  public void testSetAndGetOnExistedObjectDir() throws Exception {
    FileSystem fs = getFileSystem();
    Path path = path("testSetAndGetOnDir/dir-0/dir-1");
    fs.mkdirs(path);

    fs.setXAttr(path, XATTR_NAME, XATTR_VALUE);
    assertThrows("Not found.", IOException.class,
        () -> fs.getXAttr(path, XATTR_NAME));
  }

  @Test
  public void testGetAndListAll() throws Exception {
    FileSystem fs = getFileSystem();
    Path path = path("testGetAndListAll/file");
    fs.create(path).close();

    int size = 10;
    for (int i = 0; i < size; i++) {
      fs.setXAttr(path, XATTR_NAME + i, Bytes.toBytes("VALUE" + i));
    }

    Map<String, byte[]> result = fs.getXAttrs(path);
    assertEquals(size, result.size());
    for (int i = 0; i < size; i++) {
      assertEquals("VALUE" + i, Bytes.toString(result.get(XATTR_NAME + i)));
    }

    List<String> names = fs.listXAttrs(path);
    assertEquals(size, names.size());
    for (int i = 0; i < size; i++) {
      assertTrue(names.contains(XATTR_NAME + i));
    }
  }

  @Test
  public void testRemove() throws Exception {
    FileSystem fs = getFileSystem();
    Path path = path("testRemove/file");
    fs.create(path).close();

    int size = 10;
    for (int i = 0; i < size; i++) {
      fs.setXAttr(path, XATTR_NAME + i, Bytes.toBytes("VALUE" + i));
    }

    for (int i = 0; i < size; i++) {
      fs.removeXAttr(path, XATTR_NAME + i);
      String name = XATTR_NAME + i;
      assertThrows("Not found.", IOException.class,
          () -> fs.getXAttr(path, name));
      assertEquals(size - 1 - i, fs.listXAttrs(path).size());
    }
  }

  @Test
  public void testXAttrFlag() throws Exception {
    FileSystem fs = getFileSystem();
    Path path = path("testXAttrFlag/file");
    fs.create(path).close();

    String key = XATTR_NAME;
    byte[] value = XATTR_VALUE;
    assertThrows("The CREATE flag must be specified", IOException.class,
        () -> fs.setXAttr(path, key, value, EnumSet.of(XAttrSetFlag.REPLACE)));
    fs.setXAttr(path, key, value, EnumSet.of(XAttrSetFlag.CREATE));
    assertArrayEquals(value, fs.getXAttr(path, key));

    byte[] newValue = Bytes.toBytes("new value");
    assertThrows("The REPLACE flag must be specified", IOException.class,
        () -> fs.setXAttr(path, key, newValue, EnumSet.of(XAttrSetFlag.CREATE)));
    fs.setXAttr(path, key, newValue, EnumSet.of(XAttrSetFlag.REPLACE));
    assertArrayEquals(newValue, fs.getXAttr(path, key));
  }
}
