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
package org.apache.hadoop.hdfs.server.namenode;

import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_NAMENODE_NAME_DIR_KEY;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hdfs.server.common.HdfsConstants.StartupOption;
import org.apache.hadoop.hdfs.server.common.Storage.StorageDirectory;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeDirType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestClusterId {
  private static final Log LOG = LogFactory.getLog(TestClusterId.class);
  File hdfsDir;
  
  @Before
  public void setUp() throws IOException {
    String baseDir = System.getProperty("test.build.data", "build/test/data");

    hdfsDir = new File(baseDir, "dfs");
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete test directory '" + hdfsDir + "'");
    }
    LOG.info("hdfsdir is " + hdfsDir.getAbsolutePath());
  }
  
  @After
  public void tearDown() throws IOException {
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not tearDown test directory '" + hdfsDir + "'");
    }
  }
  
  @Test
  public void testFormatClusterIdOption() throws IOException {
    Configuration config = new Configuration();
    
    config.set(DFS_NAMENODE_NAME_DIR_KEY, new File(hdfsDir, "name").getPath());

    // 1. should fail to format without cluster id
    StartupOption.FORMAT.setClusterId("");
    try {
      NameNode.format(config);
      fail("should fail to format without cluster id");
    } catch (IllegalArgumentException e) {
      LOG.info("correctly thrown IllegalArgumentException ");
    } catch (Exception e) {
      fail("failed with a wrong exception:" + e.getLocalizedMessage());
    }

    // 2. successful format
    StartupOption.FORMAT.setClusterId("mycluster");
    try {
      NameNode.format(config);
    } catch (Exception e) {
      fail("failed to format namenode:"+e.getLocalizedMessage());
    }
    // see if cluster id not empty.
    Collection<URI> dirsToFormat = FSNamesystem.getNamespaceDirs(config);
    Collection<URI> editsToFormat = new ArrayList<URI>(0);
    FSImage fsImage = new FSImage(dirsToFormat, editsToFormat);
    
    Iterator<StorageDirectory> sdit = fsImage.dirIterator(NameNodeDirType.IMAGE);
    StorageDirectory sd = sdit.next();
    Properties props = sd.readFrom(sd.getVersionFile());
    String cid = props.getProperty("clusterID");
    LOG.info("successfully formated : sd="+sd.getCurrentDir() + ";cid="+cid);
    if(cid == null || cid.equals("")) {
      fail("didn't get new ClusterId");
    }
    

    // 3. format with existing cluster id
    StartupOption.FORMAT.setClusterId("");
    try {
      NameNode.format(config);
    } catch (Exception e) {
      fail("failed to format namenode:"+e.getLocalizedMessage());
    }
    props = sd.readFrom(sd.getVersionFile());
    String newCid = props.getProperty("clusterID");
    LOG.info("successfully formated with new cid: sd="+sd.getCurrentDir() + ";cid="+newCid);
    if(newCid == null || newCid.equals("")) {
      fail("didn't get new ClusterId");
    }
    assertTrue("should be the same", newCid.equals(cid));
  }
}
