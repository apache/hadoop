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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;
import static java.util.concurrent.TimeUnit.*;

import junit.framework.TestCase;
import static org.junit.Assert.assertArrayEquals;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration.IntegerRanges;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.net.NetUtils;
import static org.apache.hadoop.util.PlatformName.IBM_JAVA;
import org.codehaus.jackson.map.ObjectMapper;

public class TestConfiguration extends TestCase {

  private Configuration conf;
  final static String CONFIG = new File("./test-config-TestConfiguration.xml").getAbsolutePath();
  final static String CONFIG2 = new File("./test-config2-TestConfiguration.xml").getAbsolutePath();
  private static final String CONFIG_MULTI_BYTE = new File(
    "./test-config-multi-byte-TestConfiguration.xml").getAbsolutePath();
  private static final String CONFIG_MULTI_BYTE_SAVED = new File(
    "./test-config-multi-byte-saved-TestConfiguration.xml").getAbsolutePath();
  final static Random RAN = new Random();
  final static String XMLHEADER = 
            IBM_JAVA?"<?xml version=\"1.0\" encoding=\"UTF-8\"?><configuration>":
  "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><configuration>";

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
    new File(CONFIG_MULTI_BYTE).delete();
    new File(CONFIG_MULTI_BYTE_SAVED).delete();
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
  
  public void testInputStreamResource() throws Exception {
    StringWriter writer = new StringWriter();
    out = new BufferedWriter(writer);
    startConfig();
    declareProperty("prop", "A", "A");
    endConfig();
    
    InputStream in1 = new ByteArrayInputStream(writer.toString().getBytes());
    Configuration conf = new Configuration(false);
    conf.addResource(in1);
    assertEquals("A", conf.get("prop"));
    InputStream in2 = new ByteArrayInputStream(writer.toString().getBytes());
    conf.addResource(in2);
    assertEquals("A", conf.get("prop"));
  }

  /**
   * Tests use of multi-byte characters in property names and values.  This test
   * round-trips multi-byte string literals through saving and loading of config
   * and asserts that the same values were read.
   */
  public void testMultiByteCharacters() throws IOException {
    String priorDefaultEncoding = System.getProperty("file.encoding");
    try {
      System.setProperty("file.encoding", "US-ASCII");
      String name = "multi_byte_\u611b_name";
      String value = "multi_byte_\u0641_value";
      out = new BufferedWriter(new OutputStreamWriter(
        new FileOutputStream(CONFIG_MULTI_BYTE), "UTF-8"));
      startConfig();
      declareProperty(name, value, value);
      endConfig();

      Configuration conf = new Configuration(false);
      conf.addResource(new Path(CONFIG_MULTI_BYTE));
      assertEquals(value, conf.get(name));
      FileOutputStream fos = new FileOutputStream(CONFIG_MULTI_BYTE_SAVED);
      try {
        conf.writeXml(fos);
      } finally {
        IOUtils.closeStream(fos);
      }

      conf = new Configuration(false);
      conf.addResource(new Path(CONFIG_MULTI_BYTE_SAVED));
      assertEquals(value, conf.get(name));
    } finally {
      System.setProperty("file.encoding", priorDefaultEncoding);
    }
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

  public void testFinalParam() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    declareProperty("my.var", "", "", true);
    endConfig();
    Path fileResource = new Path(CONFIG);
    Configuration conf1 = new Configuration();
    conf1.addResource(fileResource);
    assertNull("my var is not null", conf1.get("my.var"));
	
    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    declareProperty("my.var", "myval", "myval", false);
    endConfig();
    fileResource = new Path(CONFIG2);

    Configuration conf2 = new Configuration(conf1);
    conf2.addResource(fileResource);
    assertNull("my var is not final", conf2.get("my.var"));
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
 
  void appendProperty(String name, String val, boolean isFinal, 
      String ... sources)
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
    for(String s : sources) {
      out.write("<source>");
      out.write(s);
      out.write("</source>");
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

  public void testGetLocalPath() throws IOException {
    Configuration conf = new Configuration();
    String[] dirs = new String[]{"a", "b", "c"};
    for (int i = 0; i < dirs.length; i++) {
      dirs[i] = new Path(System.getProperty("test.build.data"), dirs[i])
          .toString();
    }
    conf.set("dirs", StringUtils.join(dirs, ","));
    for (int i = 0; i < 1000; i++) {
      String localPath = conf.getLocalPath("dirs", "dir" + i).toString();
      assertTrue("Path doesn't end in specified dir: " + localPath,
        localPath.endsWith("dir" + i));
      assertFalse("Path has internal whitespace: " + localPath,
        localPath.contains(" "));
    }
  }
  
  public void testGetFile() throws IOException {
    Configuration conf = new Configuration();
    String[] dirs = new String[]{"a", "b", "c"};
    for (int i = 0; i < dirs.length; i++) {
      dirs[i] = new Path(System.getProperty("test.build.data"), dirs[i])
          .toString();
    }
    conf.set("dirs", StringUtils.join(dirs, ","));
    for (int i = 0; i < 1000; i++) {
      String localPath = conf.getFile("dirs", "dir" + i).toString();
      assertTrue("Path doesn't end in specified dir: " + localPath,
        localPath.endsWith("dir" + i));
      assertFalse("Path has internal whitespace: " + localPath,
        localPath.contains(" "));
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
  
  public void testWriteXml() throws IOException {
    Configuration conf = new Configuration();
    ByteArrayOutputStream baos = new ByteArrayOutputStream(); 
    conf.writeXml(baos);
    String result = baos.toString();
    assertTrue("Result has proper header", result.startsWith(XMLHEADER));
	  
    assertTrue("Result has proper footer", result.endsWith("</configuration>"));
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

  public void testRelativeIncludes() throws Exception {
    tearDown();
    String relConfig = new File("./tmp/test-config.xml").getAbsolutePath();
    String relConfig2 = new File("./tmp/test-config2.xml").getAbsolutePath();

    new File(new File(relConfig).getParent()).mkdirs();
    out = new BufferedWriter(new FileWriter(relConfig2));
    startConfig();
    appendProperty("a", "b");
    endConfig();

    out = new BufferedWriter(new FileWriter(relConfig));
    startConfig();
    // Add the relative path instead of the absolute one.
    addInclude(new File(relConfig2).getName());
    appendProperty("c", "d");
    endConfig();

    // verify that the includes file contains all properties
    Path fileResource = new Path(relConfig);
    conf.addResource(fileResource);
    assertEquals(conf.get("a"), "b");
    assertEquals(conf.get("c"), "d");

    // Cleanup
    new File(relConfig).delete();
    new File(relConfig2).delete();
    new File(new File(relConfig).getParent()).delete();
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
  
  public void testGetRangeIterator() throws Exception {
    Configuration config = new Configuration(false);
    IntegerRanges ranges = config.getRange("Test", "");
    assertFalse("Empty range has values", ranges.iterator().hasNext());
    ranges = config.getRange("Test", "5");
    Set<Integer> expected = new HashSet<Integer>(Arrays.asList(5));
    Set<Integer> found = new HashSet<Integer>();
    for(Integer i: ranges) {
      found.add(i);
    }
    assertEquals(expected, found);

    ranges = config.getRange("Test", "5-10,13-14");
    expected = new HashSet<Integer>(Arrays.asList(5,6,7,8,9,10,13,14));
    found = new HashSet<Integer>();
    for(Integer i: ranges) {
      found.add(i);
    }
    assertEquals(expected, found);
    
    ranges = config.getRange("Test", "8-12, 5- 7");
    expected = new HashSet<Integer>(Arrays.asList(5,6,7,8,9,10,11,12));
    found = new HashSet<Integer>();
    for(Integer i: ranges) {
      found.add(i);
    }
    assertEquals(expected, found);
  }

  public void testHexValues() throws IOException{
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.hex1", "0x10");
    appendProperty("test.hex2", "0xF");
    appendProperty("test.hex3", "-0x10");
    // Invalid?
    appendProperty("test.hex4", "-0x10xyz");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(16, conf.getInt("test.hex1", 0));
    assertEquals(16, conf.getLong("test.hex1", 0));
    assertEquals(15, conf.getInt("test.hex2", 0));
    assertEquals(15, conf.getLong("test.hex2", 0));
    assertEquals(-16, conf.getInt("test.hex3", 0));
    assertEquals(-16, conf.getLong("test.hex3", 0));
    try {
      conf.getLong("test.hex4", 0);
      fail("Property had invalid long value, but was read successfully.");
    } catch (NumberFormatException e) {
      // pass
    }
    try {
      conf.getInt("test.hex4", 0);
      fail("Property had invalid int value, but was read successfully.");
    } catch (NumberFormatException e) {
      // pass
    }
  }

  public void testIntegerValues() throws IOException{
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.int1", "20");
    appendProperty("test.int2", "020");
    appendProperty("test.int3", "-20");
    appendProperty("test.int4", " -20 ");
    appendProperty("test.int5", " -20xyz ");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(20, conf.getInt("test.int1", 0));
    assertEquals(20, conf.getLong("test.int1", 0));
    assertEquals(20, conf.getLongBytes("test.int1", 0));
    assertEquals(20, conf.getInt("test.int2", 0));
    assertEquals(20, conf.getLong("test.int2", 0));
    assertEquals(20, conf.getLongBytes("test.int2", 0));
    assertEquals(-20, conf.getInt("test.int3", 0));
    assertEquals(-20, conf.getLong("test.int3", 0));
    assertEquals(-20, conf.getLongBytes("test.int3", 0));
    assertEquals(-20, conf.getInt("test.int4", 0));
    assertEquals(-20, conf.getLong("test.int4", 0));
    assertEquals(-20, conf.getLongBytes("test.int4", 0));
    try {
      conf.getInt("test.int5", 0);
      fail("Property had invalid int value, but was read successfully.");
    } catch (NumberFormatException e) {
      // pass
    }
  }
  
  public void testHumanReadableValues() throws IOException {
    out = new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.humanReadableValue1", "1m");
    appendProperty("test.humanReadableValue2", "1M");
    appendProperty("test.humanReadableValue5", "1MBCDE");

    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(1048576, conf.getLongBytes("test.humanReadableValue1", 0));
    assertEquals(1048576, conf.getLongBytes("test.humanReadableValue2", 0));
    try {
      conf.getLongBytes("test.humanReadableValue5", 0);
      fail("Property had invalid human readable value, but was read successfully.");
    } catch (NumberFormatException e) {
      // pass
    }
  }

  public void testBooleanValues() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.bool1", "true");
    appendProperty("test.bool2", "false");
    appendProperty("test.bool3", "  true ");
    appendProperty("test.bool4", " false ");
    appendProperty("test.bool5", "foo");
    appendProperty("test.bool6", "TRUE");
    appendProperty("test.bool7", "FALSE");
    appendProperty("test.bool8", "");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(true, conf.getBoolean("test.bool1", false));
    assertEquals(false, conf.getBoolean("test.bool2", true));
    assertEquals(true, conf.getBoolean("test.bool3", false));
    assertEquals(false, conf.getBoolean("test.bool4", true));
    assertEquals(true, conf.getBoolean("test.bool5", true));
    assertEquals(true, conf.getBoolean("test.bool6", false));
    assertEquals(false, conf.getBoolean("test.bool7", true));
    assertEquals(false, conf.getBoolean("test.bool8", false));
  }
  
  public void testFloatValues() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.float1", "3.1415");
    appendProperty("test.float2", "003.1415");
    appendProperty("test.float3", "-3.1415");
    appendProperty("test.float4", " -3.1415 ");
    appendProperty("test.float5", "xyz-3.1415xyz");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(3.1415f, conf.getFloat("test.float1", 0.0f));
    assertEquals(3.1415f, conf.getFloat("test.float2", 0.0f));
    assertEquals(-3.1415f, conf.getFloat("test.float3", 0.0f));
    assertEquals(-3.1415f, conf.getFloat("test.float4", 0.0f));
    try {
      conf.getFloat("test.float5", 0.0f);
      fail("Property had invalid float value, but was read successfully.");
    } catch (NumberFormatException e) {
      // pass
    }
  }
  
  public void testDoubleValues() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.double1", "3.1415");
    appendProperty("test.double2", "003.1415");
    appendProperty("test.double3", "-3.1415");
    appendProperty("test.double4", " -3.1415 ");
    appendProperty("test.double5", "xyz-3.1415xyz");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals(3.1415, conf.getDouble("test.double1", 0.0));
    assertEquals(3.1415, conf.getDouble("test.double2", 0.0));
    assertEquals(-3.1415, conf.getDouble("test.double3", 0.0));
    assertEquals(-3.1415, conf.getDouble("test.double4", 0.0));
    try {
      conf.getDouble("test.double5", 0.0);
      fail("Property had invalid double value, but was read successfully.");
    } catch (NumberFormatException e) {
      // pass
    }
  }
  
  public void testGetClass() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.class1", "java.lang.Integer");
    appendProperty("test.class2", " java.lang.Integer ");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    assertEquals("java.lang.Integer", conf.getClass("test.class1", null).getCanonicalName());
    assertEquals("java.lang.Integer", conf.getClass("test.class2", null).getCanonicalName());
  }
  
  public void testGetClasses() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.classes1", "java.lang.Integer,java.lang.String");
    appendProperty("test.classes2", " java.lang.Integer , java.lang.String ");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    String[] expectedNames = {"java.lang.Integer", "java.lang.String"};
    Class<?>[] defaultClasses = {};
    Class<?>[] classes1 = conf.getClasses("test.classes1", defaultClasses);
    Class<?>[] classes2 = conf.getClasses("test.classes2", defaultClasses);
    assertArrayEquals(expectedNames, extractClassNames(classes1));
    assertArrayEquals(expectedNames, extractClassNames(classes2));
  }
  
  public void testGetStringCollection() throws IOException {
    Configuration c = new Configuration();
    c.set("x", " a, b\n,\nc ");
    Collection<String> strs = c.getTrimmedStringCollection("x");
    assertEquals(3, strs.size());
    assertArrayEquals(new String[]{ "a", "b", "c" },
                      strs.toArray(new String[0]));

    // Check that the result is mutable
    strs.add("z");

    // Make sure same is true for missing config
    strs = c.getStringCollection("does-not-exist");
    assertEquals(0, strs.size());
    strs.add("z");
  }

  public void testGetTrimmedStringCollection() throws IOException {
    Configuration c = new Configuration();
    c.set("x", "a, b, c");
    Collection<String> strs = c.getStringCollection("x");
    assertEquals(3, strs.size());
    assertArrayEquals(new String[]{ "a", " b", " c" },
                      strs.toArray(new String[0]));

    // Check that the result is mutable
    strs.add("z");

    // Make sure same is true for missing config
    strs = c.getStringCollection("does-not-exist");
    assertEquals(0, strs.size());
    strs.add("z");
  }

  private static String[] extractClassNames(Class<?>[] classes) {
    String[] classNames = new String[classes.length];
    for (int i = 0; i < classNames.length; i++) {
      classNames[i] = classes[i].getCanonicalName();
    }
    return classNames;
  }
  
  enum Dingo { FOO, BAR };
  enum Yak { RAB, FOO };
  public void testEnum() throws IOException {
    Configuration conf = new Configuration();
    conf.setEnum("test.enum", Dingo.FOO);
    assertSame(Dingo.FOO, conf.getEnum("test.enum", Dingo.BAR));
    assertSame(Yak.FOO, conf.getEnum("test.enum", Yak.RAB));
    boolean fail = false;
    try {
      conf.setEnum("test.enum", Dingo.BAR);
      Yak y = conf.getEnum("test.enum", Yak.FOO);
    } catch (IllegalArgumentException e) {
      fail = true;
    }
    assertTrue(fail);
  }

  public void testTimeDuration() {
    Configuration conf = new Configuration(false);
    conf.setTimeDuration("test.time.a", 7L, SECONDS);
    assertEquals("7s", conf.get("test.time.a"));
    assertEquals(0L, conf.getTimeDuration("test.time.a", 30, MINUTES));
    assertEquals(7L, conf.getTimeDuration("test.time.a", 30, SECONDS));
    assertEquals(7000L, conf.getTimeDuration("test.time.a", 30, MILLISECONDS));
    assertEquals(7000000L,
        conf.getTimeDuration("test.time.a", 30, MICROSECONDS));
    assertEquals(7000000000L,
        conf.getTimeDuration("test.time.a", 30, NANOSECONDS));
    conf.setTimeDuration("test.time.b", 1, DAYS);
    assertEquals("1d", conf.get("test.time.b"));
    assertEquals(1, conf.getTimeDuration("test.time.b", 1, DAYS));
    assertEquals(24, conf.getTimeDuration("test.time.b", 1, HOURS));
    assertEquals(MINUTES.convert(1, DAYS),
        conf.getTimeDuration("test.time.b", 1, MINUTES));

    // check default
    assertEquals(30L, conf.getTimeDuration("test.time.X", 30, SECONDS));
    conf.set("test.time.X", "30");
    assertEquals(30L, conf.getTimeDuration("test.time.X", 40, SECONDS));

    for (Configuration.ParsedTimeDuration ptd :
         Configuration.ParsedTimeDuration.values()) {
      conf.setTimeDuration("test.time.unit", 1, ptd.unit());
      assertEquals(1 + ptd.suffix(), conf.get("test.time.unit"));
      assertEquals(1, conf.getTimeDuration("test.time.unit", 2, ptd.unit()));
    }
  }

  public void testPattern() throws IOException {
    out = new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.pattern1", "");
    appendProperty("test.pattern2", "(");
    appendProperty("test.pattern3", "a+b");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);

    Pattern defaultPattern = Pattern.compile("x+");
    // Return default if missing
    assertEquals(defaultPattern.pattern(),
                 conf.getPattern("xxxxx", defaultPattern).pattern());
    // Return null if empty and default is null
    assertNull(conf.getPattern("test.pattern1", null));
    // Return default for empty
    assertEquals(defaultPattern.pattern(),
                 conf.getPattern("test.pattern1", defaultPattern).pattern());
    // Return default for malformed
    assertEquals(defaultPattern.pattern(),
                 conf.getPattern("test.pattern2", defaultPattern).pattern());
    // Works for correct patterns
    assertEquals("a+b",
                 conf.getPattern("test.pattern3", defaultPattern).pattern());
  }

  public void testPropertySource() throws IOException {
    out = new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.foo", "bar");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    conf.set("fs.defaultFS", "value");
    String [] sources = conf.getPropertySources("test.foo");
    assertEquals(1, sources.length);
    assertEquals(
        "Resource string returned for a file-loaded property" +
        " must be a proper absolute path",
        fileResource,
        new Path(sources[0]));
    assertArrayEquals("Resource string returned for a set() property must be " +
    		"\"programatically\"",
        new String[]{"programatically"},
        conf.getPropertySources("fs.defaultFS"));
    assertEquals("Resource string returned for an unset property must be null",
        null, conf.getPropertySources("fs.defaultFoo"));
  }
  
  public void testMultiplePropertySource() throws IOException {
    out = new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.foo", "bar", false, "a", "b", "c");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    String [] sources = conf.getPropertySources("test.foo");
    assertEquals(4, sources.length);
    assertEquals("a", sources[0]);
    assertEquals("b", sources[1]);
    assertEquals("c", sources[2]);
    assertEquals(
        "Resource string returned for a file-loaded property" +
        " must be a proper absolute path",
        fileResource,
        new Path(sources[3]));
  }

  public void testSocketAddress() throws IOException {
    Configuration conf = new Configuration();
    final String defaultAddr = "host:1";
    final int defaultPort = 2;
    InetSocketAddress addr = null;
    
    addr = conf.getSocketAddr("myAddress", defaultAddr, defaultPort);
    assertEquals(defaultAddr, NetUtils.getHostPortString(addr));
    
    conf.set("myAddress", "host2");
    addr = conf.getSocketAddr("myAddress", defaultAddr, defaultPort);
    assertEquals("host2:"+defaultPort, NetUtils.getHostPortString(addr));
    
    conf.set("myAddress", "host2:3");
    addr = conf.getSocketAddr("myAddress", defaultAddr, defaultPort);
    assertEquals("host2:3", NetUtils.getHostPortString(addr));
    
    boolean threwException = false;
    conf.set("myAddress", "bad:-port");
    try {
      addr = conf.getSocketAddr("myAddress", defaultAddr, defaultPort);
    } catch (IllegalArgumentException iae) {
      threwException = true;
      assertEquals("Does not contain a valid host:port authority: " +
                   "bad:-port (configuration property 'myAddress')",
                   iae.getMessage());
      
    } finally {
      assertTrue(threwException);
    }
  }

  public void testSetSocketAddress() throws IOException {
    Configuration conf = new Configuration();
    NetUtils.addStaticResolution("host", "127.0.0.1");
    final String defaultAddr = "host:1";
    
    InetSocketAddress addr = NetUtils.createSocketAddr(defaultAddr);    
    conf.setSocketAddr("myAddress", addr);
    assertEquals(defaultAddr, NetUtils.getHostPortString(addr));
  }
  
  public void testUpdateSocketAddress() throws IOException {
    InetSocketAddress addr = NetUtils.createSocketAddrForHost("host", 1);
    InetSocketAddress connectAddr = conf.updateConnectAddr("myAddress", addr);
    assertEquals(connectAddr.getHostName(), addr.getHostName());
    
    addr = new InetSocketAddress(1);
    connectAddr = conf.updateConnectAddr("myAddress", addr);
    assertEquals(connectAddr.getHostName(),
                 InetAddress.getLocalHost().getHostName());
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

  public static class Fake_ClassLoader extends ClassLoader {
  }

  public void testClassLoader() {
    Configuration conf = new Configuration(false);
    conf.setQuietMode(false);
    conf.setClassLoader(new Fake_ClassLoader());
    Configuration other = new Configuration(conf);
    assertTrue(other.getClassLoader() instanceof Fake_ClassLoader);
  }
  
  static class JsonConfiguration {
    JsonProperty[] properties;

    public JsonProperty[] getProperties() {
      return properties;
    }

    public void setProperties(JsonProperty[] properties) {
      this.properties = properties;
    }
  }
  
  static class JsonProperty {
    String key;
    public String getKey() {
      return key;
    }
    public void setKey(String key) {
      this.key = key;
    }
    public String getValue() {
      return value;
    }
    public void setValue(String value) {
      this.value = value;
    }
    public boolean getIsFinal() {
      return isFinal;
    }
    public void setIsFinal(boolean isFinal) {
      this.isFinal = isFinal;
    }
    public String getResource() {
      return resource;
    }
    public void setResource(String resource) {
      this.resource = resource;
    }
    String value;
    boolean isFinal;
    String resource;
  }
  
  public void testGetSetTrimmedNames() throws IOException {
    Configuration conf = new Configuration(false);
    conf.set(" name", "value");
    assertEquals("value", conf.get("name"));
    assertEquals("value", conf.get(" name"));
    assertEquals("value", conf.getRaw("  name  "));
  }

  public void testDumpConfiguration () throws IOException {
    StringWriter outWriter = new StringWriter();
    Configuration.dumpConfiguration(conf, outWriter);
    String jsonStr = outWriter.toString();
    ObjectMapper mapper = new ObjectMapper();
    JsonConfiguration jconf = 
      mapper.readValue(jsonStr, JsonConfiguration.class);
    int defaultLength = jconf.getProperties().length;
    
    // add 3 keys to the existing configuration properties
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.key1", "value1");
    appendProperty("test.key2", "value2",true);
    appendProperty("test.key3", "value3");
    endConfig();
    Path fileResource = new Path(CONFIG);
    conf.addResource(fileResource);
    out.close();
    
    outWriter = new StringWriter();
    Configuration.dumpConfiguration(conf, outWriter);
    jsonStr = outWriter.toString();
    mapper = new ObjectMapper();
    jconf = mapper.readValue(jsonStr, JsonConfiguration.class);
    int length = jconf.getProperties().length;
    // check for consistency in the number of properties parsed in Json format.
    assertEquals(length, defaultLength+3);
    
    //change few keys in another resource file
    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    appendProperty("test.key1", "newValue1");
    appendProperty("test.key2", "newValue2");
    endConfig();
    Path fileResource1 = new Path(CONFIG2);
    conf.addResource(fileResource1);
    out.close();
    
    outWriter = new StringWriter();
    Configuration.dumpConfiguration(conf, outWriter);
    jsonStr = outWriter.toString();
    mapper = new ObjectMapper();
    jconf = mapper.readValue(jsonStr, JsonConfiguration.class);
    
    // put the keys and their corresponding attributes into a hashmap for their 
    // efficient retrieval
    HashMap<String,JsonProperty> confDump = new HashMap<String,JsonProperty>();
    for(JsonProperty prop : jconf.getProperties()) {
      confDump.put(prop.getKey(), prop);
    }
    // check if the value and resource of test.key1 is changed
    assertEquals("newValue1", confDump.get("test.key1").getValue());
    assertEquals(false, confDump.get("test.key1").getIsFinal());
    assertEquals(fileResource1.toString(),
        confDump.get("test.key1").getResource());
    // check if final parameter test.key2 is not changed, since it is first 
    // loaded as final parameter
    assertEquals("value2", confDump.get("test.key2").getValue());
    assertEquals(true, confDump.get("test.key2").getIsFinal());
    assertEquals(fileResource.toString(),
        confDump.get("test.key2").getResource());
    // check for other keys which are not modified later
    assertEquals("value3", confDump.get("test.key3").getValue());
    assertEquals(false, confDump.get("test.key3").getIsFinal());
    assertEquals(fileResource.toString(),
        confDump.get("test.key3").getResource());
    // check for resource to be "Unknown" for keys which are loaded using 'set' 
    // and expansion of properties
    conf.set("test.key4", "value4");
    conf.set("test.key5", "value5");
    conf.set("test.key6", "${test.key5}");
    outWriter = new StringWriter();
    Configuration.dumpConfiguration(conf, outWriter);
    jsonStr = outWriter.toString();
    mapper = new ObjectMapper();
    jconf = mapper.readValue(jsonStr, JsonConfiguration.class);
    confDump = new HashMap<String, JsonProperty>();
    for(JsonProperty prop : jconf.getProperties()) {
      confDump.put(prop.getKey(), prop);
    }
    assertEquals("value5",confDump.get("test.key6").getValue());
    assertEquals("programatically", confDump.get("test.key4").getResource());
    outWriter.close();
  }
  
  public void testDumpConfiguratioWithoutDefaults() throws IOException {
    // check for case when default resources are not loaded
    Configuration config = new Configuration(false);
    StringWriter outWriter = new StringWriter();
    Configuration.dumpConfiguration(config, outWriter);
    String jsonStr = outWriter.toString();
    ObjectMapper mapper = new ObjectMapper();
    JsonConfiguration jconf = 
      mapper.readValue(jsonStr, JsonConfiguration.class);
    
    //ensure that no properties are loaded.
    assertEquals(0, jconf.getProperties().length);
    
    // add 2 keys
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("test.key1", "value1");
    appendProperty("test.key2", "value2",true);
    endConfig();
    Path fileResource = new Path(CONFIG);
    config.addResource(fileResource);
    out.close();
    
    outWriter = new StringWriter();
    Configuration.dumpConfiguration(config, outWriter);
    jsonStr = outWriter.toString();
    mapper = new ObjectMapper();
    jconf = mapper.readValue(jsonStr, JsonConfiguration.class);
    
    HashMap<String, JsonProperty>confDump = new HashMap<String, JsonProperty>();
    for (JsonProperty prop : jconf.getProperties()) {
      confDump.put(prop.getKey(), prop);
    }
    //ensure only 2 keys are loaded
    assertEquals(2,jconf.getProperties().length);
    //ensure the values are consistent
    assertEquals(confDump.get("test.key1").getValue(),"value1");
    assertEquals(confDump.get("test.key2").getValue(),"value2");
    //check the final tag
    assertEquals(false, confDump.get("test.key1").getIsFinal());
    assertEquals(true, confDump.get("test.key2").getIsFinal());
    //check the resource for each property
    for (JsonProperty prop : jconf.getProperties()) {
      assertEquals(fileResource.toString(),prop.getResource());
    }
  }
  
    
  public void testGetValByRegex() {
    Configuration conf = new Configuration();
    String key1 = "t.abc.key1";
    String key2 = "t.abc.key2";
    String key3 = "tt.abc.key3";
    String key4 = "t.abc.ey3";
    conf.set(key1, "value1");
    conf.set(key2, "value2");
    conf.set(key3, "value3");
    conf.set(key4, "value3");

    Map<String,String> res = conf.getValByRegex("^t\\..*\\.key\\d");
    assertTrue("Conf didn't get key " + key1, res.containsKey(key1));
    assertTrue("Conf didn't get key " + key2, res.containsKey(key2));
    assertTrue("Picked out wrong key " + key3, !res.containsKey(key3));
    assertTrue("Picked out wrong key " + key4, !res.containsKey(key4));
  }
  
  public void testSettingValueNull() throws Exception {
    Configuration config = new Configuration();
    try {
      config.set("testClassName", null);
      fail("Should throw an IllegalArgumentException exception ");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
      assertEquals(e.getMessage(),
          "The value of property testClassName must not be null");
    }
  }

  public void testSettingKeyNull() throws Exception {
    Configuration config = new Configuration();
    try {
      config.set(null, "test");
      fail("Should throw an IllegalArgumentException exception ");
    } catch (Exception e) {
      assertTrue(e instanceof IllegalArgumentException);
      assertEquals(e.getMessage(), "Property name must not be null");
    }
  }

  public void testGetClassByNameOrNull() throws Exception {
   Configuration config = new Configuration();
   Class<?> clazz = config.getClassByNameOrNull("java.lang.Object");
   assertNotNull(clazz);
  }

  public void testGetFinalParameters() throws Exception {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    declareProperty("my.var", "x", "x", true);
    endConfig();
    Path fileResource = new Path(CONFIG);
    Configuration conf = new Configuration();
    Set<String> finalParameters = conf.getFinalParameters();
    assertFalse("my.var already exists", finalParameters.contains("my.var"));
    conf.addResource(fileResource);
    assertEquals("my.var is undefined", "x", conf.get("my.var"));
    assertFalse("finalparams not copied", finalParameters.contains("my.var"));
    finalParameters = conf.getFinalParameters();
    assertTrue("my.var is not final", finalParameters.contains("my.var"));
  }

  public static void main(String[] argv) throws Exception {
    junit.textui.TestRunner.main(new String[]{
      TestConfiguration.class.getName()
    });
  }
}
