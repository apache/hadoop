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
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

import junit.framework.TestCase;


public class TestConfiguration extends TestCase {

  private Configuration conf;
  final static String CONFIG = new File("./test-config.xml").getAbsolutePath();
  final static String CONFIG2 = new File("./test-config2.xml").getAbsolutePath();

  protected void setUp() throws Exception {
    super.setUp();
    conf = new Configuration();
  }
  
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
    conf.addDefaultResource(fileResource);

    Iterator it = props.iterator();
    while(it.hasNext()) {
      Prop p = (Prop)it.next();
      System.out.println("p=" + p.name);
      String gotVal = conf.get(p.name);
      String gotRawVal = (String)conf.getObject(p.name);
      assertEq(p.val, gotRawVal);
      if(p.expectEval == UNSPEC) {
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
  ArrayList props = new ArrayList();

  void declareProperty(String name, String val, String expectEval)
    throws IOException {
    appendProperty(name, val);
    Prop p = new Prop();
    p.name = name;
    p.val = val;
    p.expectEval = expectEval;
    props.add(p);
  }

  void appendProperty(String name, String val) throws IOException {
    out.write("<property>");
    out.write("<name>");
    out.write(name);
    out.write("</name>");
    out.write("<value>");
    out.write(val);
    out.write("</value>");
    out.write("</property>\n");
  }
  
  public void testOverlay() throws IOException{
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    appendProperty("a","b");
    appendProperty("b","c");
    appendProperty("d","e");
    endConfig();

    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    appendProperty("a","b");
    appendProperty("b","d");
    endConfig();

    
    
    Path fileResource = new Path(CONFIG);
    conf.addDefaultResource(fileResource);
    
    //set dynamically something
    conf.set("c","d");
    conf.set("a","d");
    
    Configuration clone=new Configuration(conf);
    clone.addFinalResource(new Path(CONFIG2));
    
    assertEquals(clone.get("a"), "d"); 
    assertEquals(clone.get("b"), "d"); 
    assertEquals(clone.get("c"), "d"); 
    assertEquals(clone.get("d"), "e"); 
    
  }

  BufferedWriter out;
	
  public static void main(String[] argv) throws Exception {
    junit.textui.TestRunner.main(new String[]{
      TestConfiguration.class.getName()
    });
  }
}
