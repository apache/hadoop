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
package org.apache.hadoop.fs.compat.cases;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.compat.common.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@HdfsCompatCaseGroup(name = "XAttr")
public class HdfsCompatXAttr extends AbstractHdfsCompatCase {
  private Path file;

  @HdfsCompatCasePrepare
  public void prepare() throws IOException {
    this.file = makePath("file");
    HdfsCompatUtil.createFile(fs(), this.file, 0);
  }

  @HdfsCompatCaseCleanup
  public void cleanup() {
    HdfsCompatUtil.deleteQuietly(fs(), this.file, true);
  }

  @HdfsCompatCase
  public void setXAttr() throws IOException {
    final String key = "user.key";
    final byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    fs().setXAttr(file, key, value);
    Map<String, byte[]> attrs = fs().getXAttrs(file);
    assertArrayEquals(value, attrs.getOrDefault(key, new byte[0]));
  }

  @HdfsCompatCase
  public void getXAttr() throws IOException {
    final String key = "user.key";
    final byte[] value = "value".getBytes(StandardCharsets.UTF_8);
    fs().setXAttr(file, key, value);
    byte[] attr = fs().getXAttr(file, key);
    assertArrayEquals(value, attr);
  }

  @HdfsCompatCase
  public void getXAttrs() throws IOException {
    fs().setXAttr(file, "user.key1",
        "value1".getBytes(StandardCharsets.UTF_8));
    fs().setXAttr(file, "user.key2",
        "value2".getBytes(StandardCharsets.UTF_8));
    List<String> keys = new ArrayList<>();
    keys.add("user.key1");
    Map<String, byte[]> attrs = fs().getXAttrs(file, keys);
    assertEquals(1, attrs.size());
    byte[] attr = attrs.getOrDefault("user.key1", new byte[0]);
    assertArrayEquals("value1".getBytes(StandardCharsets.UTF_8), attr);
  }

  @HdfsCompatCase
  public void listXAttrs() throws IOException {
    fs().setXAttr(file, "user.key1",
        "value1".getBytes(StandardCharsets.UTF_8));
    fs().setXAttr(file, "user.key2",
        "value2".getBytes(StandardCharsets.UTF_8));
    List<String> names = fs().listXAttrs(file);
    assertEquals(2, names.size());
    assertTrue(names.contains("user.key1"));
    assertTrue(names.contains("user.key2"));
  }

  @HdfsCompatCase
  public void removeXAttr() throws IOException {
    fs().setXAttr(file, "user.key1",
        "value1".getBytes(StandardCharsets.UTF_8));
    fs().setXAttr(file, "user.key2",
        "value2".getBytes(StandardCharsets.UTF_8));
    fs().removeXAttr(file, "user.key1");
    List<String> names = fs().listXAttrs(file);
    assertEquals(1, names.size());
    assertTrue(names.contains("user.key2"));
  }
}