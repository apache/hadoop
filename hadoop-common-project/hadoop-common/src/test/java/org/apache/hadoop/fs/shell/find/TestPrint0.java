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
package org.apache.hadoop.fs.shell.find;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import java.io.IOException;

import org.apache.hadoop.fs.shell.PathData;

import java.io.PrintStream;

import org.apache.hadoop.fs.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(10)
public class TestPrint0 {
  private FileSystem mockFs;

  @BeforeEach
  public void resetMock() throws IOException {
    mockFs = MockFileSystem.setup();
  }

  // test the full path is printed to stdout with a '\0'
  @Test
  public void testPrint() throws IOException {
    Print.Print0 print = new Print.Print0();
    PrintStream out = mock(PrintStream.class);
    FindOptions options = new FindOptions();
    options.setOut(out);
    print.setOptions(options);

    String filename = "/one/two/test";
    PathData item = new PathData(filename, mockFs.getConf());
    assertEquals(Result.PASS, print.apply(item, -1));
    verify(out).print(filename + '\0');
    verifyNoMoreInteractions(out);
  }
}
