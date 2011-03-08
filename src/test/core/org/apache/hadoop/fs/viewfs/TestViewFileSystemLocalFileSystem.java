
package org.apache.hadoop.fs.viewfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileSystemTestHelper;

import org.junit.After;
import org.junit.Before;



/**
 * 
 * Test the ViewFileSystemBaseTest using a viewfs with authority: 
 *    viewfs://mountTableName/
 *    ie the authority is used to load a mount table.
 *    The authority name used is "default"
 *
 */

public class TestViewFileSystemLocalFileSystem extends ViewFileSystemBaseTest {


  @Before
  public void setUp() throws Exception {
    // create the test root on local_fs
    fsTarget = FileSystem.getLocal(new Configuration());
    super.setUp();
    
  }

  @After
  public void tearDown() throws Exception {
    fsTarget.delete(FileSystemTestHelper.getTestRootPath(fsTarget), true);
    super.tearDown();
  }
}
