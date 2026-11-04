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
package org.apache.hadoop.fs;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.test.LambdaTestUtils.intercept;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

/**
 * Tests that {@link FileContext.Util#copy(Path, Path)} refuses to copy a
 * directory into itself or its subdirectory when both paths are on the same
 * file system, including file systems with an authority such as HDFS.
 */
public class TestFileContextCopyDependencies {

  private static final Path SRC = new Path("hdfs://nn:8020/a");

  @Test
  public void testCopyToSubdirectoryOfSameAuthority() throws Exception {
    AbstractFileSystem afs = mockFileSystem("hdfs://nn:8020", SRC);
    intercept(IOException.class, "to its subdirectory",
        () -> newFileContext(afs).util()
            .copy(SRC, new Path("hdfs://nn:8020/a/b")));
    verify(afs, never()).mkdir(any(), any(), anyBoolean());
  }

  @Test
  public void testCopyToItselfSameAuthority() throws Exception {
    AbstractFileSystem afs = mockFileSystem("hdfs://nn:8020", SRC);
    intercept(IOException.class, "to itself",
        () -> newFileContext(afs).util().copy(SRC, SRC));
    verify(afs, never()).mkdir(any(), any(), anyBoolean());
  }

  @Test
  public void testCopyToOtherAuthorityIsAllowed() throws Exception {
    AbstractFileSystem afs = mockFileSystem("hdfs://nn:8020", SRC);
    Path dst = new Path("hdfs://other:8020/a/b");
    assertTrue(newFileContext(afs).util().copy(SRC, dst));
    verify(afs).mkdir(eq(dst), any(), eq(true));
  }

  @Test
  public void testCopyToSubdirectoryWithoutAuthority() throws Exception {
    Path src = new Path("file:///a");
    AbstractFileSystem afs = mockFileSystem("file:///", src);
    intercept(IOException.class, "to its subdirectory",
        () -> newFileContext(afs).util().copy(src, new Path("file:///a/b")));
    verify(afs, never()).mkdir(any(), any(), anyBoolean());
  }

  /**
   * @return a mock file system on which {@code dir} is an empty directory
   * and no other path exists.
   */
  private static AbstractFileSystem mockFileSystem(String uri, Path dir)
      throws Exception {
    AbstractFileSystem afs = mock(AbstractFileSystem.class);
    doReturn(URI.create(uri)).when(afs).getUri();
    doThrow(new FileNotFoundException()).when(afs)
        .getFileStatus(any(Path.class));
    doReturn(new FileStatus(0, true, 1, 0, 0, dir)).when(afs)
        .getFileStatus(dir);
    doReturn(new FileStatus[0]).when(afs).listStatus(any(Path.class));
    return afs;
  }

  private static FileContext newFileContext(AbstractFileSystem afs) {
    return FileContext.getFileContext(afs, new Configuration(false));
  }
}
