
package org.apache.hadoop.fs.viewfs;


import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;
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
public class TestViewFileSystemWithAuthorityLocalFileSystem extends ViewFileSystemBaseTest {
  URI schemeWithAuthority;

  @Before
  public void setUp() throws Exception {
    // create the test root on local_fs
    fsTarget = FileSystem.getLocal(new Configuration());
    super.setUp(); // this sets up conf (and fcView which we replace)

    // Now create a viewfs using a mount table called "default"
    // hence viewfs://default/
    schemeWithAuthority = 
      new URI(FsConstants.VIEWFS_SCHEME, "default", "/", null, null);
    fsView = FileSystem.get(schemeWithAuthority, conf);
  }

  @After
  public void tearDown() throws Exception {
    fsTarget.delete(FileSystemTestHelper.getTestRootPath(fsTarget), true);
    super.tearDown();
  }
 
  @Test
  public void testBasicPaths() {
    Assert.assertEquals(schemeWithAuthority,
        fsView.getUri());
    Assert.assertEquals(fsView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))),
        fsView.getWorkingDirectory());
    Assert.assertEquals(fsView.makeQualified(
        new Path("/user/" + System.getProperty("user.name"))),
        fsView.getHomeDirectory());
    Assert.assertEquals(
        new Path("/foo/bar").makeQualified(schemeWithAuthority, null),
        fsView.makeQualified(new Path("/foo/bar")));
  }
}
