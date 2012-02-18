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
package org.apache.hadoop.mapred;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.filecache.TrackerDistributedCacheManager;

/**
 * Verifies if TaskRunner.SetupWorkDir() is cleaning up files/dirs pointed
 * to by symlinks under work dir.
 */
public class TestSetupWorkDir extends TestCase {

  /**
   * Creates 1 subdirectory and 1 file under dir2. Creates 1 subdir, 1 file,
   * 1 symlink to a dir and a symlink to a file under dir1.
   * Creates dir1/subDir, dir1/file, dir2/subDir, dir2/file,
   * dir1/symlinkSubDir->dir2/subDir, dir1/symlinkFile->dir2/file.
   */
  static void createSubDirsAndSymLinks(JobConf jobConf, Path dir1, Path dir2)
       throws IOException {
    FileSystem fs = FileSystem.getLocal(jobConf);
    createSubDirAndFile(fs, dir1);
    createSubDirAndFile(fs, dir2);
    // now create symlinks under dir1 that point to file/dir under dir2
    FileUtil.symLink(dir2+"/subDir", dir1+"/symlinkSubDir");
    FileUtil.symLink(dir2+"/file", dir1+"/symlinkFile");
  }

  static void createSubDirAndFile(FileSystem fs, Path dir) throws IOException {
    Path subDir = new Path(dir, "subDir");
    fs.mkdirs(subDir);
    createFile(fs, dir, "file");
  }

  /**
   * Create a file 
   * 
   * @param fs filesystem
   * @param dir directory location of the file
   * @param fileName filename
   * @throws IOException
   */
  static void createFile(FileSystem fs, Path dir, String fileName) 
      throws IOException {
    Path p = new Path(dir, fileName);
    DataOutputStream out = fs.create(p);
    out.writeBytes("dummy input");
    out.close();    
  }
  
  void createEmptyDir(FileSystem fs, Path dir) throws IOException {
    if (fs.exists(dir)) {
      fs.delete(dir, true);
    }
    if (!fs.mkdirs(dir)) {
      throw new IOException("Unable to create directory " + dir);
    }
  }

  /**
   * Validates if TaskRunner.setupWorkDir() is properly cleaning up the
   * contents of workDir and creating tmp dir under it (even though workDir
   * contains symlinks to files/directories).
   */
  public void testSetupWorkDir() throws IOException {
    Path rootDir = new Path(System.getProperty("test.build.data",  "/tmp"),
                            "testSetupWorkDir");
    Path myWorkDir = new Path(rootDir, "./work");
    Path myTargetDir = new Path(rootDir, "./tmp");
    JobConf jConf = new JobConf();
    FileSystem fs = FileSystem.getLocal(jConf);
    createEmptyDir(fs, myWorkDir);
    createEmptyDir(fs, myTargetDir);

    // create subDirs and symlinks under work dir
    createSubDirsAndSymLinks(jConf, myWorkDir, myTargetDir);

    assertTrue("Did not create symlinks/files/dirs properly. Check "
        + myWorkDir + " and " + myTargetDir,
        (fs.listStatus(myWorkDir).length == 4) &&
        (fs.listStatus(myTargetDir).length == 2));

    // let us disable creation of symlinks in setupWorkDir()
    jConf.set(MRJobConfig.CACHE_SYMLINK, "no");

    // Deletion of myWorkDir should not affect contents of myTargetDir.
    // myTargetDir is like $user/jobcache/distcache
    TaskRunner.setupWorkDir(jConf, new File(myWorkDir.toUri().getPath()));

    // Contents of myWorkDir should be cleaned up and a tmp dir should be
    // created under myWorkDir
    assertTrue(myWorkDir + " is not cleaned up properly.",
        fs.exists(myWorkDir) && (fs.listStatus(myWorkDir).length == 1));

    // Make sure that the dir under myWorkDir is tmp
    assertTrue(fs.listStatus(myWorkDir)[0].getPath().toUri().getPath()
               .toString().equals(myWorkDir.toString() + "/tmp"));

    // Make sure that myTargetDir is not changed/deleted
    assertTrue("Dir " + myTargetDir + " seem to be modified.",
        fs.exists(myTargetDir) && (fs.listStatus(myTargetDir).length == 2));

    // cleanup
    fs.delete(rootDir, true);
  }

  /**
   * Validates distributed cache symlink getting created fine
   * 
   * @throws IOException, URISyntaxException 
   */
  public void testSetupWorkDirDistCacheSymlinkValid() 
      throws IOException, URISyntaxException {
    JobConf jConf = new JobConf();
    FileSystem fs = FileSystem.getLocal(jConf);

    Path rootDir = new Path(System.getProperty("test.build.data",  "/tmp"),
                            "testSetupWorkDirSymlinkFailure");

    // create file for DistributedCache and set it
    Path myTargetDir = new Path(rootDir, "./tmp");
    createEmptyDir(fs, myTargetDir);
    createFile(fs, myTargetDir, "cacheFile.txt");
    TrackerDistributedCacheManager.setLocalFiles(jConf, 
        (myTargetDir.toString()+Path.SEPARATOR+"cacheFile.txt"));
    assertTrue("Did not create cache file in " + myTargetDir,
        (fs.listStatus(myTargetDir).length == 1));

    // let us enable creation of symlinks in setupWorkDir()
    jConf.set(MRJobConfig.CACHE_SYMLINK, "yes");

    // add a valid symlink
    Path myWorkDir = new Path(rootDir, "./work");
    createEmptyDir(fs, myWorkDir);
    DistributedCache.addCacheFile(new URI(myWorkDir.toString() +
        Path.SEPARATOR + "file.txt#valid"), jConf);

    // setupWorkDir should create symlinks
    TaskRunner.setupWorkDir(jConf, new File(myWorkDir.toUri().getPath()));

    // myWorkDir should have 2 entries, a tmp dir and the symlink valid
    assertTrue(myWorkDir + " does not have cache symlink.",
        fs.exists(myWorkDir) && (fs.listStatus(myWorkDir).length == 2));

    // make sure work dir has symlink valid
    boolean foundValid = false;
    for (FileStatus fstat : fs.listStatus(myWorkDir)) {
      if (fstat.getPath().toUri() != null &&
            fstat.getPath().toUri().getPath().toString()
              .equals(myWorkDir.toString() + Path.SEPARATOR+ "valid")) {
        foundValid = true;
      }
    }
    
    assertTrue("Valid symlink not created", foundValid);

    // cleanup
    fs.delete(rootDir, true);
  }

  /**
   * Invalid distributed cache files errors out with IOException
   * 
   * @throws IOException, URISyntaxException 
   */
  public void testSetupWorkDirDistCacheSymlinkInvalid() 
      throws IOException, URISyntaxException {
    JobConf jConf = new JobConf();
    FileSystem fs = FileSystem.getLocal(jConf);

    Path rootDir = new Path(System.getProperty("test.build.data",  "/tmp"),
                            "testSetupWorkDirSymlinkFailure");

    // create file for DistributedCache and set it
    Path myTargetDir = new Path(rootDir, "./tmp");
    createEmptyDir(fs, myTargetDir);
    createFile(fs, myTargetDir, "cacheFile.txt");
    TrackerDistributedCacheManager.setLocalFiles(jConf, (myTargetDir.toString() +
        Path.SEPARATOR+"cacheFile.txt"));
    assertTrue("Did not create cache file in " + myTargetDir,
        (fs.listStatus(myTargetDir).length == 1));

    // let us enable creation of symlinks in setupWorkDir()
    jConf.set(MRJobConfig.CACHE_SYMLINK, "yes");

    // add an invalid symlink
    Path myWorkDir = new Path(rootDir, "./work");
    createEmptyDir(fs, myWorkDir);
    DistributedCache.addCacheFile(new URI(myWorkDir.toString() + 
        Path.SEPARATOR+"file.txt#invalid/abc"), jConf);

    // setupWorkDir should throw exception
    try {
       TaskRunner.setupWorkDir(jConf, new File(myWorkDir.toUri().getPath()));
       assertFalse("TaskRunner.setupWorkDir() did not throw exception when" +
           " given invalid cache file", true);
    } catch(IOException e) {
        // this is correct behavior
        assertTrue(myWorkDir + " does not have cache symlink.",
               fs.exists(myWorkDir) && (fs.listStatus(myWorkDir).length == 0));
    }

    // cleanup
    fs.delete(rootDir, true);
  }
}
