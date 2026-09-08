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

package org.apache.hadoop.fs.azurebfs.services;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that the XML returned by the Blob endpoint is parsed without resolving
 * external entities.
 */
public class TestAbfsBlobClientXmlParsing {

  @TempDir
  private File tempDir;

  private static final String SECRET = "top-secret-block-id";

  private String blockListWithExternalEntity(final File secretFile) {
    return "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        + "<!DOCTYPE BlockList ["
        + "<!ENTITY xxe SYSTEM \"" + secretFile.toURI() + "\">"
        + "]>"
        + "<BlockList><CommittedBlocks>"
        + "<Block><Name>&xxe;</Name><Size>1</Size></Block>"
        + "</CommittedBlocks></BlockList>";
  }

  @Test
  public void testParseBlockListResponseRejectsExternalEntity() throws Exception {
    File secretFile = new File(tempDir, "secret.txt");
    Files.write(secretFile.toPath(), SECRET.getBytes(StandardCharsets.UTF_8));

    AbfsBlobClient client = Mockito.mock(AbfsBlobClient.class);
    Mockito.doCallRealMethod().when(client).parseBlockListResponse(Mockito.any());

    InputStream stream = new ByteArrayInputStream(
        blockListWithExternalEntity(secretFile).getBytes(StandardCharsets.UTF_8));

    assertThatThrownBy(() -> client.parseBlockListResponse(stream))
        .describedAs("A DOCTYPE in a block list response must be rejected")
        .isInstanceOf(IOException.class);
  }

  @Test
  public void testParseBlockListResponseWithoutDoctype() throws Exception {
    AbfsBlobClient client = Mockito.mock(AbfsBlobClient.class);
    Mockito.doCallRealMethod().when(client).parseBlockListResponse(Mockito.any());

    String xml = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"
        + "<BlockList><CommittedBlocks>"
        + "<Block><Name>block-1</Name><Size>1</Size></Block>"
        + "<Block><Name>block-2</Name><Size>2</Size></Block>"
        + "</CommittedBlocks></BlockList>";

    List<String> blockIds = client.parseBlockListResponse(
        new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));

    assertThat(blockIds)
        .describedAs("Block ids parsed from a well formed block list")
        .containsExactly("block-1", "block-2");
  }
}
