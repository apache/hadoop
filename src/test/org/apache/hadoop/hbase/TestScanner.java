/**
 * Copyright 2006 The Apache Software Foundation
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.Enumeration;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import junit.framework.TestCase;

public class TestScanner extends TestCase {
  private static final Text FIRST_ROW = new Text();
  private static final Text[] COLS = {
      HConstants.COLUMN_FAMILY
  };
  private static final Text[] EXPLICIT_COLS = {
    HConstants.COL_REGIONINFO,
    HConstants.COL_SERVER,
    HConstants.COL_STARTCODE
  };
  
  private static final Text ROW_KEY = new Text(HGlobals.rootRegionInfo.regionName);
  private static final HRegionInfo REGION_INFO = 
    new HRegionInfo(0L, HGlobals.rootTableDesc, null, null);
  
  private static final long START_CODE = Long.MAX_VALUE;

  private HRegion region;
  private DataInputBuffer in = new DataInputBuffer();

  /** Compare the HRegionInfo we read from HBase to what we stored */
  private void validateRegionInfo(BytesWritable regionBytes) throws IOException {
    in.reset(regionBytes.get(), regionBytes.getSize());
    HRegionInfo info = new HRegionInfo();
    info.readFields(in);
    
    assertEquals(REGION_INFO.regionId, info.regionId);
    assertEquals(0, info.startKey.getLength());
    assertEquals(0, info.endKey.getLength());
    assertEquals(0, info.regionName.compareTo(REGION_INFO.regionName));
    assertEquals(0, info.tableDesc.compareTo(REGION_INFO.tableDesc));
  }
  
  /** Use a scanner to get the region info and then validate the results */
  private void scan(boolean validateStartcode, String serverName)
      throws IOException {
    
    HInternalScannerInterface scanner = null;
    TreeMap<Text, BytesWritable> results = new TreeMap<Text, BytesWritable>();
    HStoreKey key = new HStoreKey();

    Text[][] scanColumns = {
        COLS,
        EXPLICIT_COLS
    };
    
    for(int i = 0; i < scanColumns.length; i++) {
      try {
        scanner = region.getScanner(scanColumns[i], FIRST_ROW);
        while(scanner.next(key, results)) {
          assertTrue(results.containsKey(HConstants.COL_REGIONINFO));
          BytesWritable val = results.get(HConstants.COL_REGIONINFO); 
          byte[] bytes = new byte[val.getSize()];
          System.arraycopy(val.get(), 0, bytes, 0, bytes.length);
          
          validateRegionInfo(new BytesWritable(bytes));
          
          if(validateStartcode) {
            assertTrue(results.containsKey(HConstants.COL_STARTCODE));
            val = results.get(HConstants.COL_STARTCODE);
            assertNotNull(val);
            bytes = new byte[val.getSize()];
            System.arraycopy(val.get(), 0, bytes, 0, bytes.length);
            assertFalse(bytes.length == 0);
            long startCode = 
              Long.valueOf(new String(bytes, HConstants.UTF8_ENCODING));
            assertEquals(START_CODE, startCode);
          }
          
          if(serverName != null) {
            assertTrue(results.containsKey(HConstants.COL_SERVER));
            val = results.get(HConstants.COL_SERVER);
            assertNotNull(val);
            bytes = new byte[val.getSize()];
            System.arraycopy(val.get(), 0, bytes, 0, bytes.length);
            assertFalse(bytes.length == 0);
            String server = new String(bytes, HConstants.UTF8_ENCODING);
            assertEquals(0, server.compareTo(serverName));
          }
          results.clear();
        }

      } catch(IOException e) {
        e.printStackTrace();
        throw e;
      
      } finally {
        if(scanner != null) {
          try {
            scanner.close();
          
          } catch(IOException e) {
            e.printStackTrace();
          }
          scanner = null;
        }
      }
    }
  }

  /** Use get to retrieve the HRegionInfo and validate it */
  private void getRegionInfo() throws IOException {
    BytesWritable bytes = region.get(ROW_KEY, HConstants.COL_REGIONINFO);
    validateRegionInfo(bytes);  
  }
 
  /** The test! */
  @SuppressWarnings("unchecked")
  public void testScanner() throws IOException {
    MiniDFSCluster cluster = null;
    FileSystem fs = null;
    
    try {
      
      // Initialization
      
      if(System.getProperty("test.build.data") == null) {
        String dir = new File(new File("").getAbsolutePath(), "build/contrib/hbase/test").getAbsolutePath();
        System.out.println(dir);
        System.setProperty("test.build.data", dir);
      }
      Configuration conf = new HBaseConfiguration();
    
      Environment.getenv();
      if(Environment.debugging) {
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.WARN);

        ConsoleAppender consoleAppender = null;
        for(Enumeration<Appender> e = (Enumeration<Appender>)rootLogger.getAllAppenders();
            e.hasMoreElements();) {
        
          Appender a = e.nextElement();
          if(a instanceof ConsoleAppender) {
            consoleAppender = (ConsoleAppender)a;
            break;
          }
        }
        if(consoleAppender != null) {
          Layout layout = consoleAppender.getLayout();
          if(layout instanceof PatternLayout) {
            PatternLayout consoleLayout = (PatternLayout)layout;
            consoleLayout.setConversionPattern("%d %-5p [%t] %l: %m%n");
          }
        }
        Logger.getLogger("org.apache.hadoop.hbase").setLevel(Environment.logLevel);
      }
      cluster = new MiniDFSCluster(conf, 2, true, (String[])null);
      fs = cluster.getFileSystem();
      Path dir = new Path("/hbase");
      fs.mkdirs(dir);
      
      Path regionDir = HStoreFile.getHRegionDir(dir, REGION_INFO.regionName);
      fs.mkdirs(regionDir);
      
      HLog log = new HLog(fs, new Path(regionDir, "log"), conf);

      region = new HRegion(dir, log, fs, conf, REGION_INFO, null, null);
      
      // Write information to the meta table
      
      long lockid = region.startUpdate(ROW_KEY);

      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(byteStream);
      HGlobals.rootRegionInfo.write(s);
      region.put(lockid, HConstants.COL_REGIONINFO,
          new BytesWritable(byteStream.toByteArray()));
      region.commit(lockid);

      // What we just committed is in the memcache. Verify that we can get
      // it back both with scanning and get
      
      scan(false, null);
      getRegionInfo();
      
      // Close and re-open
      
      region.close();
      log.rollWriter();
      region = new HRegion(dir, log, fs, conf, REGION_INFO, null, null);

      // Verify we can get the data back now that it is on disk.
      
      scan(false, null);
      getRegionInfo();
      
      // Store some new information
 
      HServerAddress address = new HServerAddress("foo.bar.com:1234");

      lockid = region.startUpdate(ROW_KEY);

      region.put(lockid, HConstants.COL_SERVER, 
          new BytesWritable(address.toString().getBytes(HConstants.UTF8_ENCODING)));

      region.put(lockid, HConstants.COL_STARTCODE, 
          new BytesWritable(
              String.valueOf(START_CODE).getBytes(HConstants.UTF8_ENCODING)));

      region.commit(lockid);
      
      // Validate that we can still get the HRegionInfo, even though it is in
      // an older row on disk and there is a newer row in the memcache
      
      scan(true, address.toString());
      getRegionInfo();
      
      // flush cache

      region.flushcache(false);

      // Validate again
      
      scan(true, address.toString());
      getRegionInfo();

      // Close and reopen
      
      region.close();
      log.rollWriter();
      region = new HRegion(dir, log, fs, conf, REGION_INFO, null, null);

      // Validate again
      
      scan(true, address.toString());
      getRegionInfo();

      // Now update the information again

      address = new HServerAddress("bar.foo.com:4321");
      
      lockid = region.startUpdate(ROW_KEY);

      region.put(lockid, HConstants.COL_SERVER, 
          new BytesWritable(address.toString().getBytes(HConstants.UTF8_ENCODING)));

      region.commit(lockid);
      
      // Validate again
      
      scan(true, address.toString());
      getRegionInfo();

      // flush cache

      region.flushcache(false);

      // Validate again
      
      scan(true, address.toString());
      getRegionInfo();

      // Close and reopen
      
      region.close();
      log.rollWriter();
      region = new HRegion(dir, log, fs, conf, REGION_INFO, null, null);

      // Validate again
      
      scan(true, address.toString());
      getRegionInfo();

    } catch(IOException e) {
      e.printStackTrace();
      throw e;
      
    } finally {
      if(fs != null) {
        fs.close();
      }
      if(cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
