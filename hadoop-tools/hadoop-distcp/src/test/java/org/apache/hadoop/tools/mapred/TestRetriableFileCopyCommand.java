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

package org.apache.hadoop.tools.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.tools.CopyListingFileStatus;
import org.apache.hadoop.tools.mapred.CopyMapper.FileAction;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class TestRetriableFileCopyCommand {
  @SuppressWarnings("rawtypes")
  @Test
  public void testFailOnCloseError() throws Exception {
    Mapper.Context context = mock(Mapper.Context.class);
    doReturn(new Configuration()).when(context).getConfiguration();

    Exception expectedEx = new IOException("boom");
    OutputStream out = mock(OutputStream.class);
    doThrow(expectedEx).when(out).close();

    File f = File.createTempFile(this.getClass().getSimpleName(), null);
    f.deleteOnExit();
    CopyListingFileStatus stat = new CopyListingFileStatus(
        new FileStatus(1L, false, 1, 1024, 0, new Path(f.toURI())));
    
    Exception actualEx = null;
    try {
      new RetriableFileCopyCommand("testFailOnCloseError", FileAction.OVERWRITE)
        .copyBytes(stat, 0, out, 512, context);
    } catch (Exception e) {
      actualEx = e;
    }
    assertNotNull(actualEx, "close didn't fail");
    assertEquals(expectedEx, actualEx);
  }

  @Test
  @Timeout(value = 40)
  public void testGetNumBytesToRead() {
    long pos = 100;
    long buffLength = 1024;
    long fileLength = 2058;
    RetriableFileCopyCommand retriableFileCopyCommand =
            new RetriableFileCopyCommand("Testing NumBytesToRead ",
                    FileAction.OVERWRITE);
    long numBytes = retriableFileCopyCommand.getNumBytesToRead(fileLength, pos, buffLength);
    assertEquals(1024, numBytes);
    pos += numBytes;
    numBytes = retriableFileCopyCommand.getNumBytesToRead(fileLength, pos, buffLength);
    assertEquals(934, numBytes);
    pos += numBytes;
    numBytes = retriableFileCopyCommand.getNumBytesToRead(fileLength, pos, buffLength);
    assertEquals(0, numBytes);
  }
}
