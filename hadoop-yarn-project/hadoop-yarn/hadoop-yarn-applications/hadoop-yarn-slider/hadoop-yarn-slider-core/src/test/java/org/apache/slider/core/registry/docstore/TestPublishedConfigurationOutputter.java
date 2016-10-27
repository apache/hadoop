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
package org.apache.slider.core.registry.docstore;

import com.google.common.base.Charsets;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.slider.common.tools.SliderFileSystem;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.powermock.api.easymock.PowerMock;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.expect;
import static org.mockito.Matchers.anyString;
import static org.powermock.api.easymock.PowerMock.createNiceMock;

public class TestPublishedConfigurationOutputter {
  private static HashMap<String, String> config = new HashMap<>();

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @Before
  public void setup() {
    config.put("key1", "val1");
  }

  @Test
  public void testJson() throws IOException {
    PublishedConfigurationOutputter configurationOutputter =
        PublishedConfigurationOutputter.createOutputter(ConfigFormat.JSON,
            new PublishedConfiguration("description",
                config.entrySet()));

    String output = configurationOutputter.asString().replaceAll("( |\\r|\\n)",
        "");
    assert "{\"key1\":\"val1\"}".equals(output);

    File file = tmpDir.newFile();
    configurationOutputter.save(file);

    ObjectMapper mapper = new ObjectMapper();
    @SuppressWarnings("unchecked")
    Map<String, String> read = mapper.readValue(file, Map.class);
    assert 1 == read.size();
    assert "val1".equals(read.get("key1"));
  }

  @Test
  public void testXml() throws IOException {
    PublishedConfigurationOutputter configurationOutputter =
        PublishedConfigurationOutputter.createOutputter(ConfigFormat.XML,
            new PublishedConfiguration("description",
                config.entrySet()));

    String output = configurationOutputter.asString().replaceAll("( |\\r|\\n)",
        "");
    assert output.contains("<name>key1</name><value>val1</value>");

    File file = tmpDir.newFile();
    configurationOutputter.save(file);

    assert FileUtils.readFileToString(file, Charsets.UTF_8)
        .replaceAll("( |\\r|\\n)", "")
        .contains("<name>key1</name><value>val1</value>");
  }

  @Test
  public void testHadoopXml() throws IOException {
    PublishedConfigurationOutputter configurationOutputter =
        PublishedConfigurationOutputter.createOutputter(ConfigFormat.HADOOP_XML,
            new PublishedConfiguration("description",
                config.entrySet()));

    String output = configurationOutputter.asString().replaceAll("( |\\r|\\n)",
        "");
    assert output.contains("<name>key1</name><value>val1</value>");

    File file = tmpDir.newFile();
    configurationOutputter.save(file);

    assert FileUtils.readFileToString(file, Charsets.UTF_8)
        .replaceAll("( |\\r|\\n)", "")
        .contains("<name>key1</name><value>val1</value>");
  }

  @Test
  public void testProperties() throws IOException {
    PublishedConfigurationOutputter configurationOutputter =
        PublishedConfigurationOutputter.createOutputter(ConfigFormat.PROPERTIES,
            new PublishedConfiguration("description",
                config.entrySet()));

    String output = configurationOutputter.asString();
    assert output.contains("key1=val1");

    File file = tmpDir.newFile();
    configurationOutputter.save(file);

    Properties properties = new Properties();
    FileInputStream fis = null;
    try {
      fis = new FileInputStream(file);
      properties.load(fis);
    } finally {
      if (fis != null) {
        fis.close();
      }
    }
    assert 1 == properties.size();
    assert "val1".equals(properties.getProperty("key1"));
  }

  @Test
  public void testYaml() throws IOException {
    PublishedConfigurationOutputter configurationOutputter =
        PublishedConfigurationOutputter.createOutputter(ConfigFormat.YAML,
            new PublishedConfiguration("description",
                config.entrySet()));

    String output = configurationOutputter.asString().replaceAll("(\\r|\\n)",
        "");
    assert "key1: val1".equals(output);

    File file = tmpDir.newFile();
    configurationOutputter.save(file);

    Yaml yaml = new Yaml();
    FileInputStream fis = null;
    Map<String, String> read;
    try {
      fis = new FileInputStream(file);
      read = (Map<String, String>) yaml.load(fis);
    } finally {
      if (fis != null) {
        fis.close();
      }
    }
    assert 1 == read.size();
    assert "val1".equals(read.get("key1"));
  }

  @Test
  public void testEnv() throws IOException {
    HashMap<String, String> envConfig = new HashMap<>(config);
    envConfig.put("content", "content {{key1}} ");

    PublishedConfigurationOutputter configurationOutputter =
        PublishedConfigurationOutputter.createOutputter(ConfigFormat.ENV,
            new PublishedConfiguration("description",
                envConfig.entrySet()));

    String output = configurationOutputter.asString();
    assert "content val1 ".equals(output);

    File file = tmpDir.newFile();
    configurationOutputter.save(file);

    assert "content val1 ".equals(FileUtils.readFileToString(file,
        Charsets.UTF_8));
  }

  @Test
  public void testTemplate1() throws IOException {
    HashMap<String, String> templateConfig = new HashMap<>(config);
    templateConfig.put(ConfigUtils.TEMPLATE_FILE, "templateFileName");

    SliderFileSystem fileSystem = createNiceMock(SliderFileSystem.class);
    expect(fileSystem.buildResourcePath(anyString())).andReturn(new Path("path")).anyTimes();
    expect(fileSystem.isFile(anyObject(Path.class))).andReturn(true).anyTimes();
    expect(fileSystem.cat(anyObject(Path.class))).andReturn("content {{key1}}\n more ${key1} content").anyTimes();

    PowerMock.replay(fileSystem);

    ConfigUtils.prepConfigForTemplateOutputter(ConfigFormat.TEMPLATE,
        templateConfig, fileSystem, "clusterName", null);
    PublishedConfigurationOutputter configurationOutputter =
        PublishedConfigurationOutputter.createOutputter(ConfigFormat.TEMPLATE,
            new PublishedConfiguration("description",
                templateConfig.entrySet()));

    String output = configurationOutputter.asString();
    assert "content val1\n more val1 content".equals(output);

    File file = tmpDir.newFile();
    configurationOutputter.save(file);

    PowerMock.verify(fileSystem);

    assert "content val1\n more val1 content".equals(
        FileUtils.readFileToString(file, Charsets.UTF_8));
  }
}
