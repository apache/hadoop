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

package org.apache.hadoop.util;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.Objects;

import com.fasterxml.jackson.core.JsonParseException;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.test.HadoopTestBase;
import org.apache.hadoop.test.LambdaTestUtils;

/**
 * Test the JSON serialization helper.
 */
public class TestJsonSerialization extends HadoopTestBase {

  private final JsonSerialization<KeyVal> serDeser =
      new JsonSerialization<>(KeyVal.class, true, true);

  private final KeyVal source = new KeyVal("key", "1");

  private static class KeyVal implements Serializable {
    private String name;
    private String value;

    KeyVal(String name, String value) {
      this.name = name;
      this.value = value;
    }

    KeyVal() {
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("SimpleJson{");
      sb.append("name='").append(name).append('\'');
      sb.append(", value='").append(value).append('\'');
      sb.append('}');
      return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      KeyVal that = (KeyVal) o;
      return Objects.equals(name, that.name) &&
          Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
      return Objects.hash(name, value);
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getValue() {
      return value;
    }

    public void setValue(String value) {
      this.value = value;
    }
  }

  @Test
  public void testStringRoundTrip() throws Throwable {
    String wire = serDeser.toJson(source);
    KeyVal unmarshalled = serDeser.fromJson(wire);
    assertEquals("Failed to unmarshall: " + wire, source, unmarshalled);
  }

  @Test
  public void testBytesRoundTrip() throws Throwable {
    byte[] wire = serDeser.toBytes(source);
    KeyVal unmarshalled = serDeser.fromBytes(wire);
    assertEquals(source, unmarshalled);
  }

  @Test
  public void testBadBytesRoundTrip() throws Throwable {
    LambdaTestUtils.intercept(JsonParseException.class,
        "token",
        () -> serDeser.fromBytes(new byte[]{'a'}));
  }

  @Test
  public void testCloneViaJson() throws Throwable {
    KeyVal unmarshalled = serDeser.fromInstance(source);
    assertEquals(source, unmarshalled);
  }

  @Test
  public void testFileRoundTrip() throws Throwable {
    File tempFile = File.createTempFile("Keyval", ".json");
    tempFile.delete();
    try {
      serDeser.save(tempFile, source);
      assertEquals(source, serDeser.load(tempFile));
    } finally {
      tempFile.delete();
    }
  }

  @Test
  public void testEmptyFile() throws Throwable {
    File tempFile = File.createTempFile("Keyval", ".json");
    try {
      LambdaTestUtils.intercept(EOFException.class,
          "empty",
          () -> serDeser.load(tempFile));
    } finally {
      tempFile.delete();
    }
  }

  @Test
  public void testFileSystemRoundTrip() throws Throwable {
    File tempFile = File.createTempFile("Keyval", ".json");
    tempFile.delete();
    Path tempPath = new Path(tempFile.toURI());
    LocalFileSystem fs = FileSystem.getLocal(new Configuration());
    try {
      serDeser.save(fs, tempPath, source, false);
      assertEquals(source, serDeser.load(fs, tempPath));
    } finally {
      fs.delete(tempPath, false);
    }
  }

  @Test
  public void testFileSystemEmptyPath() throws Throwable {
    File tempFile = File.createTempFile("Keyval", ".json");
    Path tempPath = new Path(tempFile.toURI());
    LocalFileSystem fs = FileSystem.getLocal(new Configuration());
    try {
      LambdaTestUtils.intercept(EOFException.class,
          () -> serDeser.load(fs, tempPath));
      fs.delete(tempPath, false);
      LambdaTestUtils.intercept(FileNotFoundException.class,
          () -> serDeser.load(fs, tempPath));
    } finally {
      fs.delete(tempPath, false);
    }
  }


}
