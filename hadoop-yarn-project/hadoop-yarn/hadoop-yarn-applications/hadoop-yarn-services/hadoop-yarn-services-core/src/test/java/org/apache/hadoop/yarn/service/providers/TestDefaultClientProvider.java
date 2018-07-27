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
package org.apache.hadoop.yarn.service.providers;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.service.api.records.ConfigFile;
import org.apache.hadoop.yarn.service.exceptions.RestApiErrorMessages;
import org.apache.hadoop.yarn.service.provider.defaultImpl.DefaultClientProvider;
import org.junit.Assert;
import org.junit.Test;

public class TestDefaultClientProvider {
  private static final String EXCEPTION_PREFIX = "Should have thrown "
      + "exception: ";
  private static final String NO_EXCEPTION_PREFIX = "Should not have thrown "
      + "exception: ";

  @Test
  public void testConfigFile() throws IOException {
    DefaultClientProvider defaultClientProvider = new DefaultClientProvider();
    FileSystem mockFs = mock(FileSystem.class);
    when(mockFs.exists(anyObject())).thenReturn(true);

    String compName = "sleeper";
    ConfigFile configFile = new ConfigFile();
    configFile.setDestFile("/var/tmp/a.txt");

    try {
      defaultClientProvider.validateConfigFile(configFile, compName, mockFs);
      Assert.fail(EXCEPTION_PREFIX + " dest_file must be relative");
    } catch (IllegalArgumentException e) {
      String actualMsg = String.format(
          RestApiErrorMessages.ERROR_CONFIGFILE_DEST_FILE_FOR_COMP_NOT_ABSOLUTE,
          compName, "no", configFile.getDestFile());
      Assert.assertEquals(actualMsg, e.getLocalizedMessage());
    }

    configFile.setDestFile("../a.txt");
    try {
      defaultClientProvider.validateConfigFile(configFile, compName, mockFs);
    } catch (IllegalArgumentException e) {
      Assert.fail(NO_EXCEPTION_PREFIX + e.getLocalizedMessage());
    }
  }
}
