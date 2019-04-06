/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.submarine.client.cli;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.submarine.client.cli.param.RunJobParameters.UnderscoreConverterPropertyUtils;
import org.apache.hadoop.yarn.submarine.client.cli.param.yaml.YamlConfigFile;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Test utility class for test code that deals with YAML configuration parsing.
 */
public final class YamlConfigTestUtils {

  private YamlConfigTestUtils() {}

  static void deleteFile(File file) {
    if (file != null) {
      file.delete();
    }
  }

  static YamlConfigFile readYamlConfigFile(String filename) {
    Constructor constructor = new Constructor(YamlConfigFile.class);
    constructor.setPropertyUtils(new UnderscoreConverterPropertyUtils());
    Yaml yaml = new Yaml(constructor);
    InputStream inputStream = YamlConfigTestUtils.class
        .getClassLoader()
        .getResourceAsStream(filename);
    return yaml.loadAs(inputStream, YamlConfigFile.class);
  }

  static File createTempFileWithContents(String filename) throws IOException {
    InputStream inputStream = YamlConfigTestUtils.class
        .getClassLoader()
        .getResourceAsStream(filename);
    File targetFile = File.createTempFile("test", ".yaml");
    FileUtils.copyInputStreamToFile(inputStream, targetFile);
    return targetFile;
  }

  static File createEmptyTempFile() throws IOException {
    return File.createTempFile("test", ".yaml");
  }

}
