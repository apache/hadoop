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

package org.apache.hadoop.ozone.recon;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Paths;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.hdds.conf.OzoneConfiguration;
import org.apache.hadoop.ozone.OmUtils;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test Recon Utility methods.
 */
public class TestReconUtils {

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testGetReconDbDir() throws Exception {

    String filePath = folder.getRoot().getAbsolutePath();
    OzoneConfiguration configuration = new OzoneConfiguration();
    configuration.set("TEST_DB_DIR", filePath);

    File file = ReconUtils.getReconDbDir(configuration,
        "TEST_DB_DIR");
    Assert.assertEquals(filePath, file.getAbsolutePath());
  }

  @Test
  public void testUntarCheckpointFile() throws Exception {

    File newDir = folder.newFolder();

    File file1 = Paths.get(newDir.getAbsolutePath(), "file1")
        .toFile();
    String str = "File1 Contents";
    BufferedWriter writer = new BufferedWriter(new FileWriter(
        file1.getAbsolutePath()));
    writer.write(str);
    writer.close();

    File file2 = Paths.get(newDir.getAbsolutePath(), "file2")
        .toFile();
    str = "File2 Contents";
    writer = new BufferedWriter(new FileWriter(file2.getAbsolutePath()));
    writer.write(str);
    writer.close();

    //Create test tar file.
    File tarFile = OmUtils.createTarFile(newDir.toPath());
    File outputDir = folder.newFolder();
    ReconUtils.untarCheckpointFile(tarFile, outputDir.toPath());

    assertTrue(outputDir.isDirectory());
    assertTrue(outputDir.listFiles().length == 2);
  }

  @Test
  public void testMakeHttpCall() throws Exception {

    CloseableHttpClient httpClientMock = mock(CloseableHttpClient.class);
    String url = "http://localhost:9874/dbCheckpoint";

    CloseableHttpResponse httpResponseMock = mock(CloseableHttpResponse.class);
    when(httpClientMock.execute(any(HttpGet.class)))
        .thenReturn(httpResponseMock);

    StatusLine statusLineMock = mock(StatusLine.class);
    when(statusLineMock.getStatusCode()).thenReturn(200);
    when(httpResponseMock.getStatusLine()).thenReturn(statusLineMock);

    HttpEntity httpEntityMock = mock(HttpEntity.class);
    when(httpResponseMock.getEntity()).thenReturn(httpEntityMock);
    File file1 = Paths.get(folder.getRoot().getPath(), "file1")
        .toFile();
    BufferedWriter writer = new BufferedWriter(new FileWriter(
        file1.getAbsolutePath()));
    writer.write("File 1 Contents");
    writer.close();
    InputStream fileInputStream = new FileInputStream(file1);

    when(httpEntityMock.getContent()).thenReturn(new InputStream() {
      @Override
      public int read() throws IOException {
        return fileInputStream.read();
      }
    });

    InputStream inputStream = ReconUtils.makeHttpCall(httpClientMock, url);
    String contents = IOUtils.toString(inputStream, Charset.defaultCharset());

    assertEquals("File 1 Contents", contents);
  }

}