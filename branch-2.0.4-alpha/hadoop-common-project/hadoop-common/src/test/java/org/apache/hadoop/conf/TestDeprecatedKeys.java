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

import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.junit.Test;

import junit.framework.TestCase;

public class TestDeprecatedKeys extends TestCase {
 
  //Tests a deprecated key
  public void testDeprecatedKeys() throws Exception {
    Configuration conf = new Configuration();
    conf.set("topology.script.file.name", "xyz");
    conf.set("topology.script.file.name", "xyz");
    String scriptFile = conf.get(CommonConfigurationKeys.NET_TOPOLOGY_SCRIPT_FILE_NAME_KEY);
    assertTrue(scriptFile.equals("xyz")) ;
  }
  
  //Tests reading / writing a conf file with deprecation after setting
  public void testReadWriteWithDeprecatedKeys() throws Exception {
    Configuration conf = new Configuration();
    conf.setBoolean("old.config.yet.to.be.deprecated", true);
    Configuration.addDeprecation("old.config.yet.to.be.deprecated", 
	new String[]{"new.conf.to.replace.deprecated.conf"});
    ByteArrayOutputStream out=new ByteArrayOutputStream();
    String fileContents;
    try {
      conf.writeXml(out);
      fileContents = out.toString();
    } finally {
      out.close();
    }
    assertTrue(fileContents.contains("old.config.yet.to.be.deprecated"));
    assertTrue(fileContents.contains("new.conf.to.replace.deprecated.conf"));
  }
  
  @Test
  public void testIteratorWithDeprecatedKeysMappedToMultipleNewKeys() {
    Configuration conf = new Configuration();
    Configuration.addDeprecation("dK", new String[]{"nK1", "nK2"});
    conf.set("k", "v");
    conf.set("dK", "V");
    assertEquals("V", conf.get("dK"));
    assertEquals("V", conf.get("nK1"));
    assertEquals("V", conf.get("nK2"));
    conf.set("nK1", "VV");
    assertEquals("VV", conf.get("dK"));
    assertEquals("VV", conf.get("nK1"));
    assertEquals("VV", conf.get("nK2"));
    conf.set("nK2", "VVV");
    assertEquals("VVV", conf.get("dK"));
    assertEquals("VVV", conf.get("nK2"));
    assertEquals("VVV", conf.get("nK1"));
    boolean kFound = false;
    boolean dKFound = false;
    boolean nK1Found = false;
    boolean nK2Found = false;
    for (Map.Entry<String, String> entry : conf) {
      if (entry.getKey().equals("k")) {
        assertEquals("v", entry.getValue());
        kFound = true;
      }
      if (entry.getKey().equals("dK")) {
        assertEquals("VVV", entry.getValue());
        dKFound = true;
      }
      if (entry.getKey().equals("nK1")) {
        assertEquals("VVV", entry.getValue());
        nK1Found = true;
      }
      if (entry.getKey().equals("nK2")) {
        assertEquals("VVV", entry.getValue());
        nK2Found = true;
      }
    }
    assertTrue("regular Key not found", kFound);
    assertTrue("deprecated Key not found", dKFound);
    assertTrue("new Key 1 not found", nK1Found);
    assertTrue("new Key 2 not found", nK2Found);
  }
}
