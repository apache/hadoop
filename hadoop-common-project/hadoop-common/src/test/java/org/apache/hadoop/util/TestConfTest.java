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

import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.util.List;

import org.junit.Test;

public class TestConfTest {

  @Test
  public void testEmptyConfiguration() {
    String conf = "<configuration/>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertTrue(errors.isEmpty());
  }

  @Test
  public void testValidConfiguration() {
    String conf = "<configuration>\n"
        + "<property>\n"
        + "<name>foo</name>\n"
        + "<value>bar</value>\n"
        + "</property>\n"
        + "</configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertTrue(errors.isEmpty());
  }

  @Test
  public void testSourceDuplicationIsValid() {
    String conf = "<configuration>\n"
        + "<property source='a'>\n"
        + "<name>foo</name>\n"
        + "<value>bar</value>\n"
        + "<source>b</source>\n"
        + "<source>c</source>\n"
        + "</property>\n"
        + "</configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertTrue(errors.isEmpty());
  }

  @Test
  public void testEmptyInput() {
    String conf = "";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).startsWith("bad conf file: "));
  }

  @Test
  public void testInvalidFormat() {
    String conf = "<configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertEquals(1, errors.size());
    assertTrue(errors.get(0).startsWith("bad conf file: "));
  }

  @Test
  public void testRootElementNotConfiguration() {
    String conf = "<configurations/>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertEquals(1, errors.size());
    assertEquals("bad conf file: top-level element not <configuration>", errors.get(0));
  }

  @Test
  public void testSubElementNotProperty() {
    String conf = "<configuration>\n"
        + "<foo/>\n"
        + "</configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertEquals(1, errors.size());
    assertEquals("Line 2: element not <property>", errors.get(0));
  }

  @Test
  public void testPropertyHasNoName() {
    String conf ="<configuration>\n"
        + "<property>\n"
        + "<value>foo</value>\n"
        + "</property>\n"
        + "</configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertEquals(1, errors.size());
    assertEquals("Line 2: <property> has no <name>", errors.get(0));
  }

  @Test
  public void testPropertyHasEmptyName() {
    String conf = "<configuration>\n"
        + "<property>\n"
        + "<name></name>\n"
        + "<value>foo</value>\n"
        + "</property>\n"
        + "</configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertEquals(1, errors.size());
    assertEquals("Line 2: <property> has an empty <name>", errors.get(0));
  }

  @Test
  public void testPropertyHasNoValue() {
    String conf ="<configuration>\n"
        + "<property>\n"
        + "<name>foo</name>\n"
        + "</property>\n"
        + "</configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertEquals(1, errors.size());
    assertEquals("Line 2: <property> has no <value>", errors.get(0));
  }

  @Test
  public void testPropertyHasEmptyValue() {
    String conf = "<configuration>\n"
        + "<property>\n"
        + "<name>foo</name>\n"
        + "<value></value>\n"
        + "</property>\n"
        + "</configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertTrue(errors.isEmpty());
  }

  @Test
  public void testPropertyHasDuplicatedAttributeAndElement() {
    String conf = "<configuration>\n"
        + "<property name='foo'>\n"
        + "<name>bar</name>\n"
        + "<value>baz</value>\n"
        + "</property>\n"
        + "</configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertEquals(1, errors.size());
    assertEquals("Line 2: <property> has duplicated <name>s", errors.get(0));
  }

  @Test
  public void testPropertyHasDuplicatedElements() {
    String conf = "<configuration>\n"
        + "<property>\n"
        + "<name>foo</name>\n"
        + "<name>bar</name>\n"
        + "<value>baz</value>\n"
        + "</property>\n"
        + "</configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertEquals(1, errors.size());
    assertEquals("Line 2: <property> has duplicated <name>s", errors.get(0));
  }

  @Test
  public void testDuplicatedProperties() {
    String conf = "<configuration>\n"
        + "<property>\n"
        + "<name>foo</name>\n"
        + "<value>bar</value>\n"
        + "</property>\n"
        + "<property>\n"
        + "<name>foo</name>\n"
        + "<value>baz</value>\n"
        + "</property>\n"
        + "</configuration>";
    ByteArrayInputStream bais = new ByteArrayInputStream(conf.getBytes());
    List<String> errors = ConfTest.checkConf(bais);
    assertEquals(1, errors.size());
    assertEquals("Line 2, 6: duplicated <property>s for foo", errors.get(0));
  }

}
