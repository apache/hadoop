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
package org.apache.hadoop.hdds.conf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Rule;
import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Test class for OzoneConfiguration.
 */
public class TestOzoneConfiguration {

  private Configuration conf;

  @Rule
  public TemporaryFolder tempConfigs = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    conf = new OzoneConfiguration();
  }

  private void startConfig(BufferedWriter out) throws IOException {
    out.write("<?xml version=\"1.0\"?>\n");
    out.write("<configuration>\n");
  }

  private void endConfig(BufferedWriter out) throws IOException {
    out.write("</configuration>\n");
    out.flush();
    out.close();
  }

  @Test
  public void testGetAllPropertiesByTags() throws Exception {
    File coreDefault = tempConfigs.newFile("core-default-test.xml");
    File coreSite = tempConfigs.newFile("core-site-test.xml");
    try (BufferedWriter out = new BufferedWriter(new FileWriter(coreDefault))) {
      startConfig(out);
      appendProperty(out, "hadoop.tags.system", "YARN,HDFS,NAMENODE");
      appendProperty(out, "hadoop.tags.custom", "MYCUSTOMTAG");
      appendPropertyByTag(out, "dfs.cblock.trace.io", "false", "YARN");
      appendPropertyByTag(out, "dfs.replication", "1", "HDFS");
      appendPropertyByTag(out, "dfs.namenode.logging.level", "INFO",
          "NAMENODE");
      appendPropertyByTag(out, "dfs.random.key", "XYZ", "MYCUSTOMTAG");
      endConfig(out);

      Path fileResource = new Path(coreDefault.getAbsolutePath());
      conf.addResource(fileResource);
      Assert.assertEquals(conf.getAllPropertiesByTag("MYCUSTOMTAG")
          .getProperty("dfs.random.key"), "XYZ");
    }

    try (BufferedWriter out = new BufferedWriter(new FileWriter(coreSite))) {
      startConfig(out);
      appendProperty(out, "dfs.random.key", "ABC");
      appendProperty(out, "dfs.replication", "3");
      appendProperty(out, "dfs.cblock.trace.io", "true");
      endConfig(out);

      Path fileResource = new Path(coreSite.getAbsolutePath());
      conf.addResource(fileResource);
    }

    // Test if values are getting overridden even without tags being present
    Assert.assertEquals("3", conf.getAllPropertiesByTag("HDFS")
        .getProperty("dfs.replication"));
    Assert.assertEquals("ABC", conf.getAllPropertiesByTag("MYCUSTOMTAG")
        .getProperty("dfs.random.key"));
    Assert.assertEquals("true", conf.getAllPropertiesByTag("YARN")
        .getProperty("dfs.cblock.trace.io"));
  }

  private void appendProperty(BufferedWriter out, String name, String val)
      throws IOException {
    this.appendProperty(out, name, val, false);
  }

  private void appendProperty(BufferedWriter out, String name, String val,
                              boolean isFinal) throws IOException {
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

  private void appendPropertyByTag(BufferedWriter out, String name, String val,
                                   String tags) throws IOException {
    this.appendPropertyByTag(out, name, val, false, tags);
  }

  private void appendPropertyByTag(BufferedWriter out, String name, String val,
                                   boolean isFinal,
                                   String tag) throws IOException {
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
    out.write("<tag>");
    out.write(tag);
    out.write("</tag>");
    out.write("</property>\n");
  }
}
