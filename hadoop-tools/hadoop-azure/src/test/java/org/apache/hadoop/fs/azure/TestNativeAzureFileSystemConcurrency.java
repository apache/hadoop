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

package org.apache.hadoop.fs.azure;

import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;
import org.junit.Test;

public class TestNativeAzureFileSystemConcurrency extends AbstractWasbTestBase {
  private InMemoryBlockBlobStore backingStore;

  @Override
  public void setUp() throws Exception {
    super.setUp();
    backingStore = getTestAccount().getMockStorage().getBackingStore();
  }

  @Override
  public void tearDown() throws Exception {
    super.tearDown();
    backingStore = null;
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.createMock();
  }

  @Test
  public void testLinkBlobs() throws Exception {
    Path filePath = new Path("/inProgress");
    FSDataOutputStream outputStream = fs.create(filePath);
    // Since the stream is still open, we should see an empty link
    // blob in the backing store linking to the temporary file.
    HashMap<String, String> metadata = backingStore
        .getMetadata(AzureBlobStorageTestAccount.toMockUri(filePath));
    assertNotNull(metadata);
    String linkValue = metadata.get(AzureNativeFileSystemStore.LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY);
    linkValue = URLDecoder.decode(linkValue, "UTF-8");
    assertNotNull(linkValue);
    assertTrue(backingStore.exists(AzureBlobStorageTestAccount
        .toMockUri(linkValue)));
    // Also, WASB should say the file exists now even before we close the
    // stream.
    assertTrue(fs.exists(filePath));
    outputStream.close();
    // Now there should be no link metadata on the final file.
    metadata = backingStore.getMetadata(AzureBlobStorageTestAccount
        .toMockUri(filePath));
    assertNull(metadata
        .get(AzureNativeFileSystemStore.LINK_BACK_TO_UPLOAD_IN_PROGRESS_METADATA_KEY));
  }

  private static String toString(FileStatus[] list) {
    String[] asStrings = new String[list.length];
    for (int i = 0; i < list.length; i++) {
      asStrings[i] = list[i].getPath().toString();
    }
    return StringUtils.join(",", asStrings);
  }

  /**
   * Test to make sure that we don't expose the temporary upload folder when
   * listing at the root.
   */
  @Test
  public void testNoTempBlobsVisible() throws Exception {
    Path filePath = new Path("/inProgress");
    FSDataOutputStream outputStream = fs.create(filePath);
    // Make sure I can't see the temporary blob if I ask for a listing
    FileStatus[] listOfRoot = fs.listStatus(new Path("/"));
    assertEquals("Expected one file listed, instead got: "
        + toString(listOfRoot), 1, listOfRoot.length);
    assertEquals(fs.makeQualified(filePath), listOfRoot[0].getPath());
    outputStream.close();
  }

  /**
   * Converts a collection of exceptions to a collection of strings by getting
   * the stack trace on every exception.
   */
  private static Iterable<String> selectToString(
      final Iterable<Throwable> collection) {
    return new Iterable<String>() {
      @Override
      public Iterator<String> iterator() {
        final Iterator<Throwable> exceptionIterator = collection.iterator();
        return new Iterator<String>() {
          @Override
          public boolean hasNext() {
            return exceptionIterator.hasNext();
          }

          @Override
          public String next() {
            StringWriter stringWriter = new StringWriter();
            PrintWriter printWriter = new PrintWriter(stringWriter);
            exceptionIterator.next().printStackTrace(printWriter);
            printWriter.close();
            return stringWriter.toString();
          }

          @Override
          public void remove() {
            exceptionIterator.remove();
          }
        };
      }
    };
  }

  /**
   * Tests running starting multiple threads all doing various File system
   * operations against the same FS.
   */
  @Test
  public void testMultiThreadedOperation() throws Exception {
    for (int iter = 0; iter < 10; iter++) {
      final int numThreads = 20;
      Thread[] threads = new Thread[numThreads];
      final ConcurrentLinkedQueue<Throwable> exceptionsEncountered = new ConcurrentLinkedQueue<Throwable>();
      for (int i = 0; i < numThreads; i++) {
        final Path threadLocalFile = new Path("/myFile" + i);
        threads[i] = new Thread(new Runnable() {
          @Override
          public void run() {
            try {
              assertTrue(!fs.exists(threadLocalFile));
              OutputStream output = fs.create(threadLocalFile);
              output.write(5);
              output.close();
              assertTrue(fs.exists(threadLocalFile));
              assertTrue(fs.listStatus(new Path("/")).length > 0);
            } catch (Throwable ex) {
              exceptionsEncountered.add(ex);
            }
          }
        });
      }
      for (Thread t : threads) {
        t.start();
      }
      for (Thread t : threads) {
        t.join();
      }
      assertTrue(
          "Encountered exceptions: "
              + StringUtils.join("\r\n", selectToString(exceptionsEncountered)),
          exceptionsEncountered.isEmpty());
      tearDown();
      setUp();
    }
  }
}
