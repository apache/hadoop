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
import static org.junit.Assert.assertFalse;
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class TestClusterId {
  private static final Log LOG = LogFactory.getLog(TestClusterId.class);
  File hdfsDir;
  
  private String getClusterId(Configuration config) throws IOException {
    // see if cluster id not empty.
    Collection<URI> dirsToFormat = FSNamesystem.getNamespaceDirs(config);
    Collection<URI> editsToFormat = new ArrayList<URI>(0);
    FSImage fsImage = new FSImage(dirsToFormat, editsToFormat);
    
    Iterator<StorageDirectory> sdit = 
      fsImage.getStorage().dirIterator(NNStorage.NameNodeDirType.IMAGE);
    StorageDirectory sd = sdit.next();
    Properties props = sd.readFrom(sd.getVersionFile());
    String cid = props.getProperty("clusterID");
    LOG.info("successfully formated : sd="+sd.getCurrentDir() + ";cid="+cid);
    return cid;
  }
  
  @Before
  public void setUp() throws IOException {
    String baseDir = System.getProperty("test.build.data", "build/test/data");

    hdfsDir = new File(baseDir, "dfs");
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not delete test directory '" + 
          hdfsDir + "'");
    }
    LOG.info("hdfsdir is " + hdfsDir.getAbsolutePath());
  }
  
  @After
  public void tearDown() throws IOException {
    if ( hdfsDir.exists() && !FileUtil.fullyDelete(hdfsDir) ) {
      throw new IOException("Could not tearDown test directory '" +
          hdfsDir + "'");
    }
  }
  
  @Test
  public void testFormatClusterIdOption() throws IOException {
    Configuration config = new Configuration();
    
    config.set(DFS_NAMENODE_NAME_DIR_KEY, new File(hdfsDir, "name").getPath());

    // 1. should format without cluster id
    //StartupOption.FORMAT.setClusterId("");
    NameNode.format(config);
    // see if cluster id not empty.
    String cid = getClusterId(config);
    assertTrue("Didn't get new ClusterId", (cid != null && !cid.equals("")) );

    // 2. successful format with given clusterid
    StartupOption.FORMAT.setClusterId("mycluster");
    NameNode.format(config);
    // see if cluster id matches with given clusterid.
    cid = getClusterId(config);
    assertTrue("ClusterId didn't match", cid.equals("mycluster"));

    // 3. format without any clusterid again. It should generate new
    //clusterid.
    StartupOption.FORMAT.setClusterId("");
    NameNode.format(config);
    String newCid = getClusterId(config);
    assertFalse("ClusterId should not be the same", newCid.equals(cid));
  }
}
