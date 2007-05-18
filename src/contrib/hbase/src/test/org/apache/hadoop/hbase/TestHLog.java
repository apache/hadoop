/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase;

import java.util.TreeMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.Reader;

public class TestHLog extends HBaseTestCase implements HConstants {

  protected void setUp() throws Exception {
    super.setUp();
  }
  
  public void testAppend() throws Exception {
    Path dir = getUnitTestdir(getName());
    FileSystem fs = FileSystem.get(this.conf);
    if (fs.exists(dir)) {
      fs.delete(dir);
    }
    final int COL_COUNT = 10;
    final Text regionName = new Text("regionname");
    final Text tableName = new Text("tablename");
    final Text row = new Text("row");
    Reader reader = null;
    HLog log = new HLog(fs, dir, this.conf);
    try {
      // Write columns named 1, 2, 3, etc. and then values of single byte
      // 1, 2, 3...
      TreeMap<Text, BytesWritable> cols = new TreeMap<Text, BytesWritable>();
      for (int i = 0; i < COL_COUNT; i++) {
        cols.put(new Text(Integer.toString(i)),
          new BytesWritable(new byte[] { (byte)(i + '0') }));
      }
      long timestamp = System.currentTimeMillis();
      log.append(regionName, tableName, row, cols, timestamp);
      long logSeqId = log.startCacheFlush();
      log.completeCacheFlush(regionName, tableName, logSeqId);
      log.close();
      Path filename = log.computeFilename(log.filenum - 1);
      log = null;
      // Now open a reader on the log and assert append worked.
      reader = new SequenceFile.Reader(fs, filename, conf);
      HLogKey key = new HLogKey();
      HLogEdit val = new HLogEdit();
      for (int i = 0; i < COL_COUNT; i++) {
        reader.next(key, val);
        assertEquals(key.getRegionName(), regionName);
        assertEquals(key.getTablename(), tableName);
        assertEquals(key.getRow(), row);
        assertEquals(val.getVal().get()[0], (byte)(i + '0'));
        System.out.println(key + " " + val);
      }
      while (reader.next(key, val)) {
        // Assert only one more row... the meta flushed row.
        assertEquals(key.getRegionName(), regionName);
        assertEquals(key.getTablename(), tableName);
        assertEquals(key.getRow(), HLog.METAROW);
        assertEquals(val.getColumn(), HLog.METACOLUMN);
        assertEquals(0, val.getVal().compareTo(COMPLETE_CACHEFLUSH));
        System.out.println(key + " " + val);
      }
    } finally {
      if (log != null) {
        log.close();
      }
      if (reader != null) {
        reader.close();
      }
      if (fs.exists(dir)) {
        fs.delete(dir);
      }
    }
  }

  protected void tearDown() throws Exception {
    super.tearDown();
  }
}