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
import java.util.Iterator;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.dfs.MiniDFSCluster;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Appender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

import junit.framework.TestCase;

public class TestGet extends TestCase {
  private static final Text CONTENTS = new Text("contents:");
  private static final Text ROW_KEY = new Text(HGlobals.rootRegionInfo.regionName);

  
  private void dumpRegion(HRegion r) throws IOException {
    for(Iterator<HStore> i = r.stores.values().iterator(); i.hasNext(); ) {
      i.next().dumpMaps();
    }
  }
  
  private void verifyGet(HRegion r) throws IOException {
    // This should return a value because there is only one family member
    
    BytesWritable value = r.get(ROW_KEY, CONTENTS);
    assertNotNull(value);
    
    // This should not return a value because there are multiple family members
    
    value = r.get(ROW_KEY, HConstants.COLUMN_FAMILY);
    assertNull(value);
    
    // Find out what getFull returns
    
    TreeMap<Text, BytesWritable> values = r.getFull(ROW_KEY);
    //assertEquals(4, values.keySet().size());
    for(Iterator<Text> i = values.keySet().iterator(); i.hasNext(); ) {
      Text column = i.next();
      System.out.println(column);
      if(column.equals(HConstants.COL_SERVER)) {
        BytesWritable val = values.get(column);
        byte[] bytes = new byte[val.getSize()];
        System.arraycopy(val.get(), 0, bytes, 0, bytes.length);
        System.out.println("  " + new String(bytes, HConstants.UTF8_ENCODING));
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  public void testGet() throws IOException {
    MiniDFSCluster cluster = null;

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
      FileSystem fs = cluster.getFileSystem();
      Path dir = new Path("/hbase");
      fs.mkdirs(dir);
      
      HTableDescriptor desc = new HTableDescriptor("test", 1);
      desc.addFamily(CONTENTS);
      desc.addFamily(HConstants.COLUMN_FAMILY);
      
      HRegionInfo info = new HRegionInfo(0L, desc, null, null);
      Path regionDir = HStoreFile.getHRegionDir(dir, info.regionName);
      fs.mkdirs(regionDir);
      
      HLog log = new HLog(fs, new Path(regionDir, "log"), conf);

      HRegion r = new HRegion(dir, log, fs, conf, info, null, null);
      
      // Write information to the table
      
      long lockid = r.startUpdate(ROW_KEY);
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      DataOutputStream s = new DataOutputStream(bytes);
      CONTENTS.write(s);
      r.put(lockid, CONTENTS, new BytesWritable(bytes.toByteArray()));

      bytes.reset();
      HGlobals.rootRegionInfo.write(s);
      
      r.put(lockid, HConstants.COL_REGIONINFO, new BytesWritable(bytes.toByteArray()));
      
      r.commit(lockid);
      
      lockid = r.startUpdate(ROW_KEY);

      r.put(lockid, HConstants.COL_SERVER, 
          new BytesWritable(
              new HServerAddress("foo.bar.com:1234").toString().getBytes(HConstants.UTF8_ENCODING)
              )
      );
      
      r.put(lockid, HConstants.COL_STARTCODE, 
          new BytesWritable(
              String.valueOf(lockid).getBytes(HConstants.UTF8_ENCODING)
              )
      );
      
      r.put(lockid, new Text(HConstants.COLUMN_FAMILY + "region"), 
          new BytesWritable("region".getBytes(HConstants.UTF8_ENCODING)));

      r.commit(lockid);
      
      // Verify that get works the same from memcache as when reading from disk
      // NOTE dumpRegion won't work here because it only reads from disk.
      
      verifyGet(r);
      
      // Close and re-open region, forcing updates to disk
      
      r.close();
      log.rollWriter();
      r = new HRegion(dir, log, fs, conf, info, null, null);
      
      // Read it back
      
      dumpRegion(r);
      verifyGet(r);
      
      // Update one family member and add a new one
      
      lockid = r.startUpdate(ROW_KEY);

      r.put(lockid, new Text(HConstants.COLUMN_FAMILY + "region"),
          new BytesWritable("region2".getBytes()));

      r.put(lockid, HConstants.COL_SERVER, 
          new BytesWritable(
              new HServerAddress("bar.foo.com:4321").toString().getBytes(HConstants.UTF8_ENCODING)
              )
      );
      
      r.put(lockid, new Text(HConstants.COLUMN_FAMILY + "junk"),
          new BytesWritable("junk".getBytes()));
      
      r.commit(lockid);

      verifyGet(r);
      
      // Close region and re-open it
      
      r.close();
      log.rollWriter();
      r = new HRegion(dir, log, fs, conf, info, null, null);

      // Read it back
      
      dumpRegion(r);
      verifyGet(r);

      // Close region once and for all
      
      r.close();
      
    } catch(IOException e) {
      e.printStackTrace();
      throw e;
      
    } finally {
      if(cluster != null) {
        cluster.shutdown();
      }
    }
  }
}
