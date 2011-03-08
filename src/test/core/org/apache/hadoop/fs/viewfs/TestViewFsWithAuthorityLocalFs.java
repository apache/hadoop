
package org.apache.hadoop.fs.viewfs;


import java.net.URI;

import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * Test the ViewFsBaseTest using a viewfs with authority: 
 *    viewfs://mountTableName/
 *    ie the authority is used to load a mount table.
 *    The authority name used is "default"
 *
 */

public class TestViewFsWithAuthorityLocalFs extends ViewFsBaseTest {
  URI schemeWithAuthority;

  @Before
  public void setUp() throws Exception {
    // create the test root on local_fs
    fcTarget = FileContext.getLocalFSFileContext();
    super.setUp(); // this sets up conf (and fcView which we replace)
    
    // Now create a viewfs using a mount table called "default"
    // hence viewfs://default/
    schemeWithAuthority = 
      new URI(FsConstants.VIEWFS_SCHEME, "default", "/", null, null);
    fcView = FileContext.getFileContext(schemeWithAuthority, conf);  
  }

  @After
  public void tearDown() throws Exception {
    super.tearDown();
  }
  
  @Test
  public void testBasicPaths() {
      Assert.assertEquals(schemeWithAuthority,
          fcView.getDefaultFileSystem().getUri());
      Assert.assertEquals(fcView.makeQualified(
          new Path("/user/" + System.getProperty("user.name"))),
          fcView.getWorkingDirectory());
      Assert.assertEquals(fcView.makeQualified(
          new Path("/user/" + System.getProperty("user.name"))),
          fcView.getHomeDirectory());
      Assert.assertEquals(
          new Path("/foo/bar").makeQualified(schemeWithAuthority, null),
          fcView.makeQualified(new Path("/foo/bar")));
  }
}
