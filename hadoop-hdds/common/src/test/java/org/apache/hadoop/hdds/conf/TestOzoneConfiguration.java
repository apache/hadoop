/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdds.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class TestOzoneConfiguration {

  private Configuration conf;
  final static String CONFIG = new File("./test-config-TestConfiguration.xml").getAbsolutePath();
  final static String CONFIG_CORE = new File("./core-site.xml").getAbsolutePath();

  private BufferedWriter out;

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
  }

  @After
  public void tearDown() throws Exception {
    if(out != null) {
      out.close();
    }
    new File(CONFIG).delete();
    new File(CONFIG_CORE).delete();
  }

  private void startConfig() throws IOException {
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
  }

  private void endConfig() throws IOException{
    out.write("</configuration>\n");
    out.flush();
    out.close();
  }

  @Test
  public void testGetAllPropertiesByTags() throws Exception {

    try{
      out = new BufferedWriter(new FileWriter(CONFIG));
      startConfig();
      appendProperty("hadoop.tags.system", "YARN,HDFS,NAMENODE");
      appendProperty("hadoop.tags.custom", "MYCUSTOMTAG");
      appendPropertyByTag("dfs.cblock.trace.io", "false", "YARN");
      appendPropertyByTag("dfs.replication", "1", "HDFS");
      appendPropertyByTag("dfs.namenode.logging.level", "INFO", "NAMENODE");
      appendPropertyByTag("dfs.random.key", "XYZ", "MYCUSTOMTAG");
      endConfig();

      Path fileResource = new Path(CONFIG);
      conf.addResource(fileResource);
      assertEq(conf.getAllPropertiesByTag("MYCUSTOMTAG").getProperty("dfs.random.key"), "XYZ");
    } finally {
      out.close();
    }
    try {
      out = new BufferedWriter(new FileWriter(CONFIG_CORE));
      startConfig();
      appendProperty("dfs.random.key", "ABC");
      appendProperty("dfs.replication", "3");
      appendProperty("dfs.cblock.trace.io", "true");
      endConfig();

      Path fileResource = new Path(CONFIG_CORE);
      conf.addResource(fileResource);

    } finally {
      out.close();
    }

    // Test if values are getting overridden even without tags being present
    assertEq("3", conf.getAllPropertiesByTag("HDFS").getProperty("dfs.replication"));
    assertEq("ABC", conf.getAllPropertiesByTag("MYCUSTOMTAG").getProperty("dfs.random.key"));
    assertEq("true", conf.getAllPropertiesByTag("YARN").getProperty("dfs.cblock.trace.io"));
  }

  private void appendProperty(String name, String val) throws IOException {
    this.appendProperty(name, val, false, new String[0]);
  }

  private void appendProperty(String name, String val, boolean isFinal, String... sources) throws IOException {
    this.out.write("<property>");
    this.out.write("<name>");
    this.out.write(name);
    this.out.write("</name>");
    this.out.write("<value>");
    this.out.write(val);
    this.out.write("</value>");
    if(isFinal) {
      this.out.write("<final>true</final>");
    }

    String[] var5 = sources;
    int var6 = sources.length;

    for(int var7 = 0; var7 < var6; ++var7) {
      String s = var5[var7];
      this.out.write("<source>");
      this.out.write(s);
      this.out.write("</source>");
    }

    this.out.write("</property>\n");
  }

  private void appendPropertyByTag(String name, String val, String tags, String... sources) throws IOException {
    this.appendPropertyByTag(name, val, false, tags, sources);
  }

  private void appendPropertyByTag(String name, String val, boolean isFinal, String tag, String... sources) throws IOException {
    this.out.write("<property>");
    this.out.write("<name>");
    this.out.write(name);
    this.out.write("</name>");
    this.out.write("<value>");
    this.out.write(val);
    this.out.write("</value>");
    if(isFinal) {
      this.out.write("<final>true</final>");
    }

    String[] var6 = sources;
    int var7 = sources.length;

    for(int var8 = 0; var8 < var7; ++var8) {
      String s = var6[var8];
      this.out.write("<source>");
      this.out.write(s);
      this.out.write("</source>");
    }

    this.out.write("<tag>");
    this.out.write(tag);
    this.out.write("</tag>");
    this.out.write("</property>\n");
  }

  private static void assertEq(Object a, Object b) {
    System.out.println("assertEq: " + a + ", " + b);
    Assert.assertEquals(a, b);
  }
}
