package org.apache.hadoop.fs.viewfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileContextTestHelper;
import org.apache.hadoop.fs.FsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.viewfs.ConfigUtil;


/**
 * This class is for  setup and teardown for viewFs so that
 * it can be tested via the standard FileContext tests.
 * 
 * If tests launched via ant (build.xml) the test root is absolute path
 * If tests launched via eclipse, the test root is 
 * is a test dir below the working directory. (see FileContextTestHelper).
 * Since viewFs has no built-in wd, its wd is /user/<username>.
 * 
 * We set up fc to be the viewFs with mount point for 
 * /<firstComponent>" pointing to the local file system's testdir 
 */
public class ViewFsTestSetup {


   /* 
   * return the ViewFS File context to be used for tests
   */
  static public FileContext setupForViewFsLocalFs() throws Exception {
    /**
     * create the test root on local_fs - the  mount table will point here
     */
    FileContext fclocal = FileContext.getLocalFSFileContext();
    Path targetOfTests = FileContextTestHelper.getTestRootPath(fclocal);
    // In case previous test was killed before cleanup
    fclocal.delete(targetOfTests, true);
    
    fclocal.mkdir(targetOfTests, FileContext.DEFAULT_PERM, true);
  
    String srcTestFirstDir;
    if (FileContextTestHelper.TEST_ROOT_DIR.startsWith("/")) {
      int indexOf2ndSlash = FileContextTestHelper.TEST_ROOT_DIR.indexOf('/', 1);
      srcTestFirstDir = FileContextTestHelper.TEST_ROOT_DIR.substring(0, indexOf2ndSlash);
    } else {
      srcTestFirstDir = "/user"; 
  
    }
    //System.out.println("srcTestFirstDir=" + srcTestFirstDir);
  
    // Set up the defaultMT in the config with mount point links
    // The test dir is root is below  /user/<userid>
    Configuration conf = new Configuration();
    ConfigUtil.addLink(conf, srcTestFirstDir,
        targetOfTests.toUri());
    
    FileContext fc = FileContext.getFileContext(FsConstants.VIEWFS_URI, conf);
    //System.out.println("SRCOfTests = "+ getTestRootPath(fc, "test"));
    //System.out.println("TargetOfTests = "+ targetOfTests.toUri());
    return fc;
  }

  /**
   * 
   * delete the test directory in the target local fs
   */
  static public void tearDownForViewFsLocalFs() throws Exception {
    FileContext fclocal = FileContext.getLocalFSFileContext();
    Path targetOfTests = FileContextTestHelper.getTestRootPath(fclocal);
    fclocal.delete(targetOfTests, true);
  }

}
