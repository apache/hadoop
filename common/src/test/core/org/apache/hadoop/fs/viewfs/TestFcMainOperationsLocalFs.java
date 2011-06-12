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
package org.apache.hadoop.fs.viewfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextMainOperationsBaseTest;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;

import org.junit.After;
import org.junit.Before;


public class TestFcMainOperationsLocalFs  extends 
  FileContextMainOperationsBaseTest {

  FileContext fclocal;
  Path targetOfTests;

  @Before
  public void setUp() throws Exception {
    /**
     * create the test root on local_fs - the  mount table will point here
     */
    fclocal = FileContext.getLocalFSFileContext();
    targetOfTests = FileContextTestHelper.getTestRootPath(fclocal);
    // In case previous test was killed before cleanup
    fclocal.delete(targetOfTests, true);
    
    fclocal.mkdir(targetOfTests, FileContext.DEFAULT_PERM, true);

    
    
    
    // We create mount table so that the test root on the viewFs points to 
    // to the test root on the target.
    // DOing this helps verify the FileStatus.path.
    //
    // The test root by default when running eclipse 
    // is a test dir below the working directory. 
    // (see FileContextTestHelper).
    // Since viewFs has no built-in wd, its wd is /user/<username>.
    // If this test launched via ant (build.xml) the test root is absolute path
    
    String srcTestRoot;
    if (FileContextTestHelper.TEST_ROOT_DIR.startsWith("/")) {
      srcTestRoot = FileContextTestHelper.TEST_ROOT_DIR;
    } else {
      srcTestRoot = "/user/"  + System.getProperty("user.name") + "/" +
      FileContextTestHelper.TEST_ROOT_DIR;
    }

    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, srcTestRoot,
        targetOfTests.toUri());
    
    fc = FileContext.getFileContext(FsConstants.VIEWFS_URI, conf);
    //System.out.println("SRCOfTests = "+ FileContextTestHelper.getTestRootPath(fc, "test"));
    //System.out.println("TargetOfTests = "+ targetOfTests.toUri());
    super.setUp();
  }
  
  @After
  public void tearDown() throws Exception {
    super.tearDown();
    fclocal.delete(targetOfTests, true);
  }
}