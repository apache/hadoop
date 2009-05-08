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
package org.apache.hadoop.conf;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.DataInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.Random;

import junit.framework.TestCase;

import org.apache.hadoop.fs.Path;


public class TestConfiguration extends TestCase {

  private Configuration conf;
  final static String CONFIG = new File("./test-config.xml").getAbsolutePath();
  final static String CONFIG2 = new File("./test-config2.xml").getAbsolutePath();
  final static Random RAN = new Random();

  @Override
  protected void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
  }
  
  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
    new File(CONFIG).delete();
    new File(CONFIG2).delete();
  }
  
  private void startConfig() throws IOException{
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
  }

  private void endConfig() throws IOException{
    out.write("</configuration>\n");
    out.close();
  }

  private void addInclude(String filename) throws IOException{
    out.write("<xi:include href=\"" + filename + "\" xmlns:xi=\"http://www.w3.org/2001/XInclude\"  />\n ");
  }

  public void testVariableSubstitution() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    declareProperty("my.int", "${intvar}", "42");
    declareProperty("intvar", "42", "42");
    declareProperty("my.base", "/tmp/${user.name}", UNSPEC);
    declareProperty("my.file", "hello", "hello");
    declareProperty("my.suffix", ".txt", ".txt");
    declareProperty("my.relfile", "${my.file}${my.suffix}", "hello.txt");
    declareProperty("my.fullfile", "${my.base}/${my.file}${my.suffix}", UNSPEC);
    // check that undefined variables are returned as-is
    declareProperty("my.failsexpand", "a${my.undefvar}b", "a${my.undefvar}b");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);

    for (Prop p : props) {
      System.out.println("p=" + p.name);
      String gotVal = conf.get(p.name);
      String gotRawVal = conf.getRaw(p.name);
      assertEq(p.val, gotRawVal);
      if (p.expectEval == UNSPEC) {
        // expansion is system-dependent (uses System properties)
        // can't do exact match so just check that all variables got expanded
        assertTrue(gotVal != null && -1 == gotVal.indexOf("${"));
      } else {
        assertEq(p.expectEval, gotVal);
      }
    }
      
    // check that expansion also occurs for getInt()
    assertTrue(conf.getInt("intvar", -1) == 42);
    assertTrue(conf.getInt("my.int", -1) == 42);
  }
    
  public static void assertEq(Object a, Object b) {
    System.out.println("assertEq: " + a + ", " + b);
    assertEquals(a, b);
  }

  static class Prop {
    String name;
    String val;
    String expectEval;
  }

  final String UNSPEC = null;
  ArrayList<Prop> props = new ArrayList<Prop>();

  void declareProperty(String name, String val, String expectEval)
    throws IOException {
    declareProperty(name, val, expectEval, false);
  }

  void declareProperty(String name, String val, String expectEval,
                       boolean isFinal)
    throws IOException {
    appendProperty(name, val, isFinal);
    Prop p = new Prop();
    p.name = name;
    p.val = val;
    p.expectEval = expectEval;
    props.add(p);
  }

  void appendProperty(String name, String val) throws IOException {
    appendProperty(name, val, false);
  }
 
  void appendProperty(String name, String val, boolean isFinal)
    throws IOException {
    out.write("<property>");
    out.write("<name>");
    out.write(name);
    out.write("</name>");
    out.write("<value>");
    out.write(val);
    out.write("</value>");
    if (isFinal) {
      out.write("<final>true</final>");
    }
    out.write("</property>\n");
  }
  
  public void testOverlay() throws IOException{
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("a","b");
    appendProperty("b","c");
    appendProperty("d","e");
    appendProperty("e","f", true);
    endConfig();

    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    appendProperty("a","b");
    appendProperty("b","d");
    appendProperty("e","e");
    endConfig();
    
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    
    //set dynamically something
    conf.set("c","d");
    conf.set("a","d");
    
    Configuration clone=new Configuration(conf);
    clone.addResource(new Path(CONFIG2));
    
    assertEquals(clone.get("a"), "d"); 
    assertEquals(clone.get("b"), "d"); 
    assertEquals(clone.get("c"), "d"); 
    assertEquals(clone.get("d"), "e"); 
    assertEquals(clone.get("e"), "f"); 
    
  }
  
  public void testCommentsInValue() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("my.comment", "this <!--comment here--> contains a comment");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    //two spaces one after "this", one before "contains"
    assertEquals("this  contains a comment", conf.get("my.comment"));
  }
  
  public void testTrim() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    String[] whitespaces = {"", " ", "\n", "\t"};
    String[] name = new String[100];
    for(int i = 0; i < name.length; i++) {
      name[i] = "foo" + i;
      StringBuilder prefix = new StringBuilder(); 
      StringBuilder postfix = new StringBuilder(); 
      for(int j = 0; j < 3; j++) {
        prefix.append(whitespaces[RAN.nextInt(whitespaces.length)]);
        postfix.append(whitespaces[RAN.nextInt(whitespaces.length)]);
      }
      
      appendProperty(prefix + name[i] + postfix, name[i] + ".value");
    }
    endConfig();

    conf.addResource(new Path(CONFIG));
    for(String n : name) {
      assertEquals(n + ".value", conf.get(n));
    }
  }

  public void testToString() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    
    String expectedOutput = 
      "Configuration: core-default.xml, core-site.xml, " + 
      fileResource.toString();
    assertEquals(expectedOutput, conf.toString());
  }
  
  public void testIncludes() throws Exception {
    tearDown();
    System.out.println("XXX testIncludes");
    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    appendProperty("a","b");
    appendProperty("c","d");
    endConfig();

    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    addInclude(CONFIG2);
    appendProperty("e","f");
    appendProperty("g","h");
    endConfig();

    // verify that the includes file contains all properties
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(conf.get("a"), "b"); 
    assertEquals(conf.get("c"), "d"); 
    assertEquals(conf.get("e"), "f"); 
    assertEquals(conf.get("g"), "h"); 
    tearDown();
  }

  BufferedWriter out;
	
  public void testIntegerRanges() {
    Configuration conf = new Configuration();
    conf.set("first", "-100");
    conf.set("second", "4-6,9-10,27");
    conf.set("third", "34-");
    Configuration.IntegerRanges range = conf.getRange("first", null);
    System.out.println("first = " + range);
    assertEquals(true, range.isIncluded(0));
    assertEquals(true, range.isIncluded(1));
    assertEquals(true, range.isIncluded(100));
    assertEquals(false, range.isIncluded(101));
    range = conf.getRange("second", null);
    System.out.println("second = " + range);
    assertEquals(false, range.isIncluded(3));
    assertEquals(true, range.isIncluded(4));
    assertEquals(true, range.isIncluded(6));
    assertEquals(false, range.isIncluded(7));
    assertEquals(false, range.isIncluded(8));
    assertEquals(true, range.isIncluded(9));
    assertEquals(true, range.isIncluded(10));
    assertEquals(false, range.isIncluded(11));
    assertEquals(false, range.isIncluded(26));
    assertEquals(true, range.isIncluded(27));
    assertEquals(false, range.isIncluded(28));
    range = conf.getRange("third", null);
    System.out.println("third = " + range);
    assertEquals(false, range.isIncluded(33));
    assertEquals(true, range.isIncluded(34));
    assertEquals(true, range.isIncluded(100000000));
  }

  public void testHexValues() throws IOException{
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.hex1", "0x10");
    appendProperty("test.hex2", "0xF");
    appendProperty("test.hex3", "-0x10");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(16, conf.getInt("test.hex1", 0));
    assertEquals(16, conf.getLong("test.hex1", 0));
    assertEquals(15, conf.getInt("test.hex2", 0));
    assertEquals(15, conf.getLong("test.hex2", 0));
    assertEquals(-16, conf.getInt("test.hex3", 0));
    assertEquals(-16, conf.getLong("test.hex3", 0));

  }

  public void testIntegerValues() throws IOException{
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.int1", "20");
    appendProperty("test.int2", "020");
    appendProperty("test.int3", "-20");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(20, conf.getInt("test.int1", 0));
    assertEquals(20, conf.getLong("test.int1", 0));
    assertEquals(20, conf.getInt("test.int2", 0));
    assertEquals(20, conf.getLong("test.int2", 0));
    assertEquals(-20, conf.getInt("test.int3", 0));
    assertEquals(-20, conf.getLong("test.int3", 0));
  }
	
  public void testReload() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.key1", "final-value1", true);
    appendProperty("test.key2", "value2");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    
    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    appendProperty("test.key1", "value1");
    appendProperty("test.key3", "value3");
    endConfig();
    Path fileResource1 = new Path(CONFIG2);
    conf.addResource(fileResource1);
    
    // add a few values via set.
    conf.set("test.key3", "value4");
    conf.set("test.key4", "value5");
    
    assertEquals("final-value1", conf.get("test.key1"));
    assertEquals("value2", conf.get("test.key2"));
    assertEquals("value4", conf.get("test.key3"));
    assertEquals("value5", conf.get("test.key4"));
    
    // change values in the test file...
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.key1", "final-value1");
    appendProperty("test.key3", "final-value3", true);
    endConfig();
    
    conf.reloadConfiguration();
    assertEquals("value1", conf.get("test.key1"));
    // overlayed property overrides.
    assertEquals("value4", conf.get("test.key3"));
    assertEquals(null, conf.get("test.key2"));
    assertEquals("value5", conf.get("test.key4"));
  }

  public void testSize() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    conf.set("b", "B");
    assertEquals(2, conf.size());
  }

  public void testClear() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set("a", "A");
    conf.set("b", "B");
    conf.clear();
    assertEquals(0, conf.size());
    assertFalse(conf.iterator().hasNext());
  }

  public static void main(String[] argv) throws Exception {
    junit.textui.TestRunner.main(new String[]{
      TestConfiguration.class.getName()
    });
  }
}
