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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

 
public class TestConfigurationDeprecation {
  private Configuration conf;
  final static String CONFIG = new File("./test-config.xml").getAbsolutePath();
  final static String CONFIG2 = 
    new File("./test-config2.xml").getAbsolutePath();
  final static String CONFIG3 = 
    new File("./test-config3.xml").getAbsolutePath();
  BufferedWriter out;

  @Before
  public void setUp() throws Exception {
    conf = new Configuration(false);
  }

  @After
  public void tearDown() throws Exception {
    new File(CONFIG).delete();
    new File(CONFIG2).delete();
    new File(CONFIG3).delete();
  }
  
  private void startConfig() throws IOException{
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
  }

  private void endConfig() throws IOException{
    out.write("</configuration>\n");
    out.close();
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
  
  private void addDeprecationToConfiguration() {
    Configuration.addDeprecation("old.key1", new String[]{"new.key1"});
    Configuration.addDeprecation("old.key2", new String[]{"new.key2"});
    Configuration.addDeprecation("old.key3", new String[]{"new.key3"});
    Configuration.addDeprecation("old.key4", new String[]{"new.key4"});
    Configuration.addDeprecation("old.key5", new String[]{"new.key5"});
    Configuration.addDeprecation("old.key6", new String[]{"new.key6"});
    Configuration.addDeprecation("old.key7", new String[]{"new.key7"});
    Configuration.addDeprecation("old.key8", new String[]{"new.key8"});
    Configuration.addDeprecation("old.key9", new String[]{"new.key9"});
    Configuration.addDeprecation("old.key10", new String[]{"new.key10"});
    Configuration.addDeprecation("old.key11", new String[]{"new.key11"});
    Configuration.addDeprecation("old.key12", new String[]{"new.key12"});
    Configuration.addDeprecation("old.key13", new String[]{"new.key13"});
    Configuration.addDeprecation("old.key14", new String[]{"new.key14"});
    Configuration.addDeprecation("old.key15", new String[]{"new.key15"});
    Configuration.addDeprecation("old.key16", new String[]{"new.key16"});
    Configuration.addDeprecation("A", new String[]{"B"});
    Configuration.addDeprecation("C", new String[]{"D"});
    Configuration.addDeprecation("E", new String[]{"F"});
    Configuration.addDeprecation("G", new String[]{"H","I"});
  }
  
  /**
   * This test is to check the precedence order between being final and 
   * deprecation.Based on the order of occurrence of deprecated key and 
   * its corresponding mapping key, various cases arise.
   * The precedence order being followed is:
   * 1. Final Parameter 
   * 2. Deprecated key's value.
   * @throws IOException 
   * 
   * @throws IOException
   * @throws ClassNotFoundException 
   */
  @Test
  public void testDeprecation() throws IOException {
    out=new BufferedWriter(new FileWriter(CONFIG));
    startConfig();
    // load keys with default values. Some of them are set to final to
    // test the precedence order between deprecation and being final
    appendProperty("new.key1","default.value1",true);
    appendProperty("new.key2","default.value2");
    appendProperty("new.key3","default.value3",true);
    appendProperty("new.key4","default.value4");
    appendProperty("new.key5","default.value5",true);
    appendProperty("new.key6","default.value6");
    appendProperty("new.key7","default.value7",true);
    appendProperty("new.key8","default.value8");
    appendProperty("new.key9","default.value9");
    appendProperty("new.key10","default.value10");
    appendProperty("new.key11","default.value11");
    appendProperty("new.key12","default.value12");
    appendProperty("new.key13","default.value13");
    appendProperty("new.key14","default.value14");
    appendProperty("new.key15","default.value15");
    appendProperty("new.key16","default.value16");
    endConfig();
    Path fileResource = new Path(CONFIG);
    addDeprecationToConfiguration();
    conf.addResource(fileResource);
    
    out=new BufferedWriter(new FileWriter(CONFIG2));
    startConfig();
    // add keys that are tested while they are loaded just after their 
    // corresponding default values
    appendProperty("old.key1","old.value1",true);
    appendProperty("old.key2","old.value2",true);
    appendProperty("old.key3","old.value3");
    appendProperty("old.key4","old.value4");
    appendProperty("new.key5","new.value5",true);
    appendProperty("new.key6","new.value6",true);
    appendProperty("new.key7","new.value7");
    appendProperty("new.key8","new.value8");
    
    // add keys that are tested while they are loaded first and are followed by
    // loading of their corresponding deprecated or replacing key
    appendProperty("new.key9","new.value9",true);
    appendProperty("new.key10","new.value10");
    appendProperty("new.key11","new.value11",true);
    appendProperty("new.key12","new.value12");
    appendProperty("old.key13","old.value13",true);
    appendProperty("old.key14","old.value14");
    appendProperty("old.key15","old.value15",true);
    appendProperty("old.key16","old.value16");
    endConfig();
    Path fileResource1 = new Path(CONFIG2);
    conf.addResource(fileResource1);
    
    out=new BufferedWriter(new FileWriter(CONFIG3));
    startConfig();
    // add keys which are already loaded by the corresponding replacing or 
    // deprecated key.
    appendProperty("old.key9","old.value9",true);
    appendProperty("old.key10","old.value10",true);
    appendProperty("old.key11","old.value11");
    appendProperty("old.key12","old.value12");
    appendProperty("new.key13","new.value13",true);
    appendProperty("new.key14","new.value14",true);
    appendProperty("new.key15","new.value15");
    appendProperty("new.key16","new.value16");
    appendProperty("B", "valueB");
    endConfig();
    Path fileResource2 = new Path(CONFIG3);
    conf.addResource(fileResource2);
    
    // get the values. Also check for consistency in get of old and new keys, 
    // when they are set to final or non-final
    // Key - the key that is being loaded
    // isFinal - true if the key is marked as final
    // prev.occurrence - key that most recently loaded the current key 
    //                   with its value.
    // isPrevFinal - true if key corresponding to 
    //               prev.occurrence is marked as final.
    
    // Key-deprecated , isFinal-true, prev.occurrence-default.xml,
    // isPrevFinal-true
    assertEquals("old.value1", conf.get("new.key1"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key1"), conf.get("new.key1"));
    // Key-deprecated , isFinal-true, prev.occurrence-default.xml,
    // isPrevFinal-false
    assertEquals("old.value2", conf.get("new.key2"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key2"), conf.get("new.key2"));
    // Key-deprecated , isFinal-false, prev.occurrence-default.xml,
    // isPrevFinal-true
    assertEquals("default.value3", conf.get("new.key3"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key3"), conf.get("new.key3"));
    // Key-deprecated , isFinal-false, prev.occurrence-default.xml,
    // isPrevFinal-false
    assertEquals("old.value4", conf.get("new.key4"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key4"), conf.get("new.key4"));
    // Key-site.xml , isFinal-true, prev.occurrence-default.xml,
    // isPrevFinal-true
    assertEquals("default.value5", conf.get("new.key5"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key5"), conf.get("new.key5"));
    // Key-site.xml , isFinal-true, prev.occurrence-default.xml,
    // isPrevFinal-false
    assertEquals("new.value6",conf.get("new.key6"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key6"), conf.get("new.key6"));
    // Key-site.xml , isFinal-false, prev.occurrence-default.xml,
    // isPrevFinal-true
    assertEquals("default.value7", conf.get("new.key7"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key7"), conf.get("new.key7"));
    // Key-site.xml , isFinal-false, prev.occurrence-default.xml,
    // isPrevFinal-false
    assertEquals("new.value8",conf.get("new.key8"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key8"), conf.get("new.key8"));
    // Key-deprecated , isFinal-true, prev.occurrence-site.xml,
    // isPrevFinal-true
    assertEquals("old.value9", conf.get("new.key9"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key9"), conf.get("new.key9"));
    // Key-deprecated , isFinal-true, prev.occurrence-site.xml,
    // isPrevFinal-false
    assertEquals("old.value10", conf.get("new.key10"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key10"), conf.get("new.key10"));
    // Key-deprecated , isFinal-false, prev.occurrence-site.xml,
    // isPrevFinal-true
    assertEquals("new.value11", conf.get("new.key11"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key11"), conf.get("new.key11"));
    // Key-deprecated , isFinal-false, prev.occurrence-site.xml,
    // isPrevFinal-false
    assertEquals("old.value12", conf.get("new.key12"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key12"), conf.get("new.key12"));
    // Key-site.xml , isFinal-true, prev.occurrence-deprecated,
    // isPrevFinal-true
    assertEquals("old.value13", conf.get("new.key13"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key13"), conf.get("new.key13"));
    // Key-site.xml , isFinal-true, prev.occurrence-deprecated,
    // isPrevFinal-false
    assertEquals("new.value14", conf.get("new.key14"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key14"), conf.get("new.key14"));
    // Key-site.xml , isFinal-false, prev.occurrence-deprecated,
    // isPrevFinal-true
    assertEquals("old.value15", conf.get("new.key15"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key15"), conf.get("new.key15"));
    // Key-site.xml , isFinal-false, prev.occurrence-deprecated,
    // isPrevFinal-false
    assertEquals("old.value16", conf.get("new.key16"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("old.key16"), conf.get("new.key16"));
    
    // ensure that reloadConfiguration doesn't deprecation information
    conf.reloadConfiguration();
    assertEquals(conf.get("A"), "valueB");
    // check consistency in get of old and new keys
    assertEquals(conf.get("A"), conf.get("B"));
    
    // check for consistency in get and set of deprecated and corresponding 
    // new keys from the user code
    // set old key
    conf.set("C", "valueC");
    // get new key
    assertEquals("valueC",conf.get("D"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("C"), conf.get("D"));
    
    // set new key
    conf.set("F","valueF");
    // get old key
    assertEquals("valueF", conf.get("E"));
    // check consistency in get of old and new keys
    assertEquals(conf.get("E"), conf.get("F"));
    
    conf.set("G", "valueG");
    assertEquals("valueG", conf.get("G"));
    assertEquals("valueG", conf.get("H"));
    assertEquals("valueG", conf.get("I"));
  }
  
  // Ensure that wasDeprecatedKeySet returns the correct result under
  // the three code paths possible 
  @Test
  public void testWasDeprecatedKeySet() {
    Configuration.addDeprecation("oldKeyA", new String [] { "newKeyA"});
    Configuration.addDeprecation("oldKeyB", new String [] { "newKeyB"});
    
    // Used the deprecated key rather than the new, therefore should trigger
    conf.set("oldKeyA", "AAA");
    assertEquals("AAA", conf.get("newKeyA"));
    assertTrue(conf.deprecatedKeyWasSet("oldKeyA"));
  
    // There is a deprecated key, but it wasn't specified. Therefore, don't trigger
    conf.set("newKeyB", "AndrewBird");
    assertEquals("AndrewBird", conf.get("newKeyB"));
    assertFalse(conf.deprecatedKeyWasSet("oldKeyB"));
    
    // Not a deprecated key, therefore shouldn't trigger deprecatedKeyWasSet
    conf.set("BrandNewKey", "BrandNewValue");
    assertEquals("BrandNewValue", conf.get("BrandNewKey"));
    assertFalse(conf.deprecatedKeyWasSet("BrandNewKey"));
  }

}
