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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.azure.integration.AzureTestUtils;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.PermissionStatus;

/**
 * Handle OOB IO into a shared container.
 */
public class ITestAzureConcurrentOutOfBandIo extends AbstractWasbTestBase {

  private static final Logger LOG =
      LoggerFactory.getLogger(ITestAzureConcurrentOutOfBandIo.class);

  // Class constants.
  static final int DOWNLOAD_BLOCK_SIZE = 8 * 1024 * 1024;
  static final int UPLOAD_BLOCK_SIZE = 4 * 1024 * 1024;
  static final int BLOB_SIZE = 32 * 1024 * 1024;

  // Number of blocks to be written before flush.
  static final int NUMBER_OF_BLOCKS = 2;

  @Override
  protected AzureBlobStorageTestAccount createTestAccount() throws Exception {
    return AzureBlobStorageTestAccount.createOutOfBandStore(
        UPLOAD_BLOCK_SIZE, DOWNLOAD_BLOCK_SIZE);
  }

  class DataBlockWriter implements Runnable {

    Thread runner;
    AzureBlobStorageTestAccount writerStorageAccount;
    String key;
    boolean done = false;

    /**
     * Constructor captures the test account.
     * 
     * @param testAccount
     */
    public DataBlockWriter(AzureBlobStorageTestAccount testAccount, String key) {
      writerStorageAccount = testAccount;
      this.key = key;
    }

    /**
     * Start writing blocks to Azure storage.
     */
    public void startWriting() {
      runner = new Thread(this); // Create the block writer thread.
      runner.start(); // Start the block writer thread.
    }

    /**
     * Stop writing blocks to Azure storage.
     */
    public void stopWriting() {
      done = true;
    }

    /**
     * Implementation of the runnable interface. The run method is a tight loop
     * which repeatedly updates the blob with a 4 MB block.
     */
    public void run() {
      byte[] dataBlockWrite = new byte[UPLOAD_BLOCK_SIZE];

      OutputStream outputStream = null;

      try {
        for (int i = 0; !done; i++) {
          // Write two 4 MB blocks to the blob.
          //
          outputStream = writerStorageAccount.getStore().storefile(
              key,
              new PermissionStatus("", "", FsPermission.getDefault()),
              key);

          Arrays.fill(dataBlockWrite, (byte) (i % 256));
          for (int j = 0; j < NUMBER_OF_BLOCKS; j++) {
            outputStream.write(dataBlockWrite);
          }

          outputStream.flush();
          outputStream.close();
        }
      } catch (AzureException e) {
        LOG.error("DatablockWriter thread encountered a storage exception."
            + e.getMessage(), e);
      } catch (IOException e) {
        LOG.error("DatablockWriter thread encountered an I/O exception."
            + e.getMessage(), e);
      }
    }
  }

  @Test
  public void testReadOOBWrites() throws Exception {

    byte[] dataBlockWrite = new byte[UPLOAD_BLOCK_SIZE];
    byte[] dataBlockRead = new byte[UPLOAD_BLOCK_SIZE];

    // Write to blob to make sure it exists.
    //
   // Write five 4 MB blocks to the blob. To ensure there is data in the blob before
   // reading.  This eliminates the race between the reader and writer threads.
    String key = "WASB_String" + AzureTestUtils.getForkID() + ".txt";
    OutputStream outputStream = testAccount.getStore().storefile(
       key,
       new PermissionStatus("", "", FsPermission.getDefault()),
           key);
   Arrays.fill(dataBlockWrite, (byte) 255);
   for (int i = 0; i < NUMBER_OF_BLOCKS; i++) {
     outputStream.write(dataBlockWrite);
   }

   outputStream.flush();
   outputStream.close();

   // Start writing blocks to Azure store using the DataBlockWriter thread.
    DataBlockWriter writeBlockTask = new DataBlockWriter(testAccount, key);
    writeBlockTask.startWriting();
   int count = 0;

   for (int i = 0; i < 5; i++) {
     try(InputStream inputStream = testAccount.getStore().retrieve(key)) {
        count = 0;
        int c = 0;

        while (c >= 0) {
          c = inputStream.read(dataBlockRead, 0, UPLOAD_BLOCK_SIZE);
          if (c < 0) {
            break;
          }

          // Counting the number of bytes.
          count += c;
        }
     } catch (IOException e) {
       System.out.println(e.getCause().toString());
       e.printStackTrace();
       fail();
     }
   }

    // Stop writing blocks.
    writeBlockTask.stopWriting();

    // Validate that a block was read.
    assertEquals(NUMBER_OF_BLOCKS * UPLOAD_BLOCK_SIZE, count);
  }
}
