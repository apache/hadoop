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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem.Statistics;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.util.concurrent.Uninterruptibles;

import static org.apache.hadoop.fs.FileContextTestHelper.*;

/**
 * <p>
 *   Base class to test {@link FileContext} Statistics.
 * </p>
 */
public abstract class FCStatisticsBaseTest {
  
  static protected int blockSize = 512;
  static protected int numBlocks = 1;
  
  protected final FileContextTestHelper fileContextTestHelper = new FileContextTestHelper();

  //fc should be set appropriately by the deriving test.
  protected static FileContext fc = null;
  
  @Test(timeout=60000)
  public void testStatisticsOperations() throws Exception {
    final Statistics stats = new Statistics("file");
    Assert.assertEquals(0L, stats.getBytesRead());
    Assert.assertEquals(0L, stats.getBytesWritten());
    Assert.assertEquals(0, stats.getWriteOps());
    stats.incrementBytesWritten(1000);
    Assert.assertEquals(1000L, stats.getBytesWritten());
    Assert.assertEquals(0, stats.getWriteOps());
    stats.incrementWriteOps(123);
    Assert.assertEquals(123, stats.getWriteOps());
    
    Thread thread = new Thread() {
      @Override
      public void run() {
        stats.incrementWriteOps(1);
      }
    };
    thread.start();
    Uninterruptibles.joinUninterruptibly(thread);
    Assert.assertEquals(124, stats.getWriteOps());
    // Test copy constructor and reset function
    Statistics stats2 = new Statistics(stats);
    stats.reset();
    Assert.assertEquals(0, stats.getWriteOps());
    Assert.assertEquals(0L, stats.getBytesWritten());
    Assert.assertEquals(0L, stats.getBytesRead());
    Assert.assertEquals(124, stats2.getWriteOps());
    Assert.assertEquals(1000L, stats2.getBytesWritten());
    Assert.assertEquals(0L, stats2.getBytesRead());
  }

  @Test
  public void testStatistics() throws IOException, URISyntaxException {
    URI fsUri = getFsUri();
    Statistics stats = FileContext.getStatistics(fsUri);
    Assert.assertEquals(0, stats.getBytesRead());
    Path filePath = fileContextTestHelper .getTestRootPath(fc, "file1");
    createFile(fc, filePath, numBlocks, blockSize);

    Assert.assertEquals(0, stats.getBytesRead());
    verifyWrittenBytes(stats);
    FSDataInputStream fstr = fc.open(filePath);
    byte[] buf = new byte[blockSize];
    int bytesRead = fstr.read(buf, 0, blockSize);
    fstr.read(0, buf, 0, blockSize);
    Assert.assertEquals(blockSize, bytesRead);
    verifyReadBytes(stats);
    verifyWrittenBytes(stats);
    verifyReadBytes(FileContext.getStatistics(getFsUri()));
    Map<URI, Statistics> statsMap = FileContext.getAllStatistics();
    URI exactUri = getSchemeAuthorityUri();
    verifyWrittenBytes(statsMap.get(exactUri));
    fc.delete(filePath, true);
  }

  /**
   * Bytes read may be different for different file systems. This method should
   * throw assertion error if bytes read are incorrect.
   * 
   * @param stats
   */
  protected abstract void verifyReadBytes(Statistics stats);

  /**
   * Bytes written may be different for different file systems. This method should
   * throw assertion error if bytes written are incorrect.
   * 
   * @param stats
   */
  protected abstract void verifyWrittenBytes(Statistics stats);
  
  /**
   * Returns the filesystem uri. Should be set
   * @return URI
   */
  protected abstract URI getFsUri();

  protected URI getSchemeAuthorityUri() {
    URI uri = getFsUri();
    String SchemeAuthString = uri.getScheme() + "://";
    if (uri.getAuthority() == null) {
      SchemeAuthString += "/";
    } else {
      SchemeAuthString += uri.getAuthority();
    }
    return URI.create(SchemeAuthString);
  }
}
