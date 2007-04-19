package org.apache.hadoop.fs.s3;

import java.io.IOException;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

public abstract class S3FileSystemBaseTest extends TestCase {
  
  private static final int BLOCK_SIZE = 128;
  
  private S3FileSystem s3FileSystem;

  private byte[] data;

  abstract FileSystemStore getFileSystemStore() throws IOException;

  @Override
  protected void setUp() throws IOException {
    Configuration conf = new Configuration();
    
    s3FileSystem = new S3FileSystem(getFileSystemStore());
    s3FileSystem.initialize(URI.create(conf.get("test.fs.s3.name")), conf);
    
    data = new byte[BLOCK_SIZE * 2];
    for (int i = 0; i < data.length; i++) {
      data[i] = (byte) (i % 10);
    }
  }

  @Override
  protected void tearDown() throws Exception {
    s3FileSystem.purge();
    s3FileSystem.close();
  }
  
  public void testWorkingDirectory() throws Exception {

    Path homeDir = new Path("/user/", System.getProperty("user.name"));
    assertEquals(homeDir, s3FileSystem.getWorkingDirectory());

    s3FileSystem.setWorkingDirectory(new Path("."));
    assertEquals(homeDir, s3FileSystem.getWorkingDirectory());

    s3FileSystem.setWorkingDirectory(new Path(".."));
    assertEquals(new Path("/user/"), s3FileSystem.getWorkingDirectory());

    s3FileSystem.setWorkingDirectory(new Path("hadoop"));
    assertEquals(new Path("/user/hadoop"), s3FileSystem.getWorkingDirectory());

    s3FileSystem.setWorkingDirectory(new Path("/test/hadoop"));
    assertEquals(new Path("/test/hadoop"), s3FileSystem.getWorkingDirectory());

  }

  public void testMkdirs() throws Exception {
    Path testDir = new Path("/test/hadoop");
    assertFalse(s3FileSystem.exists(testDir));
    assertFalse(s3FileSystem.isDirectory(testDir));
    assertFalse(s3FileSystem.isFile(testDir));

    assertTrue(s3FileSystem.mkdirs(testDir));

    assertTrue(s3FileSystem.exists(testDir));
    assertTrue(s3FileSystem.isDirectory(testDir));
    assertFalse(s3FileSystem.isFile(testDir));

    Path parentDir = testDir.getParent();
    assertTrue(s3FileSystem.exists(parentDir));
    assertTrue(s3FileSystem.isDirectory(parentDir));
    assertFalse(s3FileSystem.isFile(parentDir));

    Path grandparentDir = parentDir.getParent();
    assertTrue(s3FileSystem.exists(grandparentDir));
    assertTrue(s3FileSystem.isDirectory(grandparentDir));
    assertFalse(s3FileSystem.isFile(grandparentDir));
  }

  public void testListPathsRaw() throws Exception {
    Path[] testDirs = { new Path("/test/hadoop/a"), new Path("/test/hadoop/b"),
                        new Path("/test/hadoop/c/1"), };
    assertNull(s3FileSystem.listPaths(testDirs[0]));

    for (Path path : testDirs) {
      assertTrue(s3FileSystem.mkdirs(path));
    }

    Path[] paths = s3FileSystem.listPaths(new Path("/"));

    assertEquals(1, paths.length);
    assertEquals(new Path("/test"), paths[0]);

    paths = s3FileSystem.listPaths(new Path("/test"));
    assertEquals(1, paths.length);
    assertEquals(new Path("/test/hadoop"), paths[0]);

    paths = s3FileSystem.listPaths(new Path("/test/hadoop"));
    assertEquals(3, paths.length);
    assertEquals(new Path("/test/hadoop/a"), paths[0]);
    assertEquals(new Path("/test/hadoop/b"), paths[1]);
    assertEquals(new Path("/test/hadoop/c"), paths[2]);

    paths = s3FileSystem.listPaths(new Path("/test/hadoop/a"));
    assertEquals(0, paths.length);
  }

  public void testWriteReadAndDeleteEmptyFile() throws Exception {
    writeReadAndDelete(0);
  }

  public void testWriteReadAndDeleteHalfABlock() throws Exception {
    writeReadAndDelete(BLOCK_SIZE / 2);
  }

  public void testWriteReadAndDeleteOneBlock() throws Exception {
    writeReadAndDelete(BLOCK_SIZE);
  }
  
  public void testWriteReadAndDeleteOneAndAHalfBlocks() throws Exception {
    writeReadAndDelete(BLOCK_SIZE + BLOCK_SIZE / 2);
  }
  
  public void testWriteReadAndDeleteTwoBlocks() throws Exception {
    writeReadAndDelete(BLOCK_SIZE * 2);
  }
  
  
  private void writeReadAndDelete(int len) throws IOException {
    Path path = new Path("/test/hadoop/file");
    
    s3FileSystem.mkdirs(path.getParent());

    FSDataOutputStream out = s3FileSystem.create(path, false,
                                                 s3FileSystem.getConf().getInt("io.file.buffer.size", 4096), 
                                                 (short) 1, BLOCK_SIZE);
    out.write(data, 0, len);
    out.close();

    assertTrue("Exists", s3FileSystem.exists(path));
    
    assertEquals("Block size", Math.min(len, BLOCK_SIZE), s3FileSystem.getBlockSize(path));

    assertEquals("Length", len, s3FileSystem.getLength(path));

    FSDataInputStream in = s3FileSystem.open(path);
    byte[] buf = new byte[len];

    in.readFully(0, buf);

    assertEquals(len, buf.length);
    for (int i = 0; i < buf.length; i++) {
      assertEquals("Position " + i, data[i], buf[i]);
    }
    
    assertTrue("Deleted", s3FileSystem.delete(path));
    
    assertFalse("No longer exists", s3FileSystem.exists(path));

  }

  public void testOverwrite() throws IOException {
    Path path = new Path("/test/hadoop/file");
    
    s3FileSystem.mkdirs(path.getParent());

    createEmptyFile(path);
    
    assertTrue("Exists", s3FileSystem.exists(path));
    assertEquals("Length", BLOCK_SIZE, s3FileSystem.getLength(path));
    
    try {
      s3FileSystem.create(path, false,
                          s3FileSystem.getConf().getInt("io.file.buffer.size", 4096),
                          (short) 1, 128);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // Expected
    }
    
    FSDataOutputStream out = s3FileSystem.create(path, true,
                                                 s3FileSystem.getConf().getInt("io.file.buffer.size", 4096), 
                                                 (short) 1, BLOCK_SIZE);
    out.write(data, 0, BLOCK_SIZE / 2);
    out.close();
    
    assertTrue("Exists", s3FileSystem.exists(path));
    assertEquals("Length", BLOCK_SIZE / 2, s3FileSystem.getLength(path));
    
  }

  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = new Path("/test/hadoop/file");    
    createEmptyFile(path);
    
    assertTrue("Exists", s3FileSystem.exists(path));
    assertEquals("Length", BLOCK_SIZE, s3FileSystem.getLength(path));
    assertTrue("Parent exists", s3FileSystem.exists(path.getParent()));
  }

  public void testDeleteNonExistentFile() throws IOException {
    Path path = new Path("/test/hadoop/file");    
    assertFalse("Doesn't exist", s3FileSystem.exists(path));
    assertFalse("No deletion", s3FileSystem.delete(path));
  }

  public void testDeleteDirectory() throws IOException {
    Path subdir = new Path("/test/hadoop");
    Path dir = subdir.getParent();
    Path root = dir.getParent();
    s3FileSystem.mkdirs(subdir);
    Path file1 = new Path(dir, "file1");
    Path file2 = new Path(subdir, "file2");
    
    createEmptyFile(file1);
    createEmptyFile(file2);
    
    assertTrue("root exists", s3FileSystem.exists(root));
    assertTrue("dir exists", s3FileSystem.exists(dir));
    assertTrue("file1 exists", s3FileSystem.exists(file1));
    assertTrue("subdir exists", s3FileSystem.exists(subdir));
    assertTrue("file2 exists", s3FileSystem.exists(file2));
    
    assertTrue("Delete", s3FileSystem.delete(dir));

    assertTrue("root exists", s3FileSystem.exists(root));
    assertFalse("dir exists", s3FileSystem.exists(dir));
    assertFalse("file1 exists", s3FileSystem.exists(file1));
    assertFalse("subdir exists", s3FileSystem.exists(subdir));
    assertFalse("file2 exists", s3FileSystem.exists(file2));
    
  }
  
  public void testRenameNonExistentPath() throws Exception {
    Path src = new Path("/test/hadoop/path");
    Path dst = new Path("/test/new/newpath");
    rename(src, dst, false, false, false);
  }

  public void testRenameFileMoveToNonExistentDirectory() throws Exception {
    Path src = new Path("/test/hadoop/file");
    createEmptyFile(src);
    Path dst = new Path("/test/new/newfile");
    rename(src, dst, false, true, false);
  }

  public void testRenameFileMoveToExistingDirectory() throws Exception {
    Path src = new Path("/test/hadoop/file");
    createEmptyFile(src);
    Path dst = new Path("/test/new/newfile");
    s3FileSystem.mkdirs(dst.getParent());
    rename(src, dst, true, false, true);
  }

  public void testRenameFileAsExistingFile() throws Exception {
    Path src = new Path("/test/hadoop/file");
    createEmptyFile(src);
    Path dst = new Path("/test/new/newfile");
    createEmptyFile(dst);
    rename(src, dst, false, true, true);
  }

  public void testRenameFileAsExistingDirectory() throws Exception {
    Path src = new Path("/test/hadoop/file");
    createEmptyFile(src);
    Path dst = new Path("/test/new/newdir");
    s3FileSystem.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertTrue("Destination changed", s3FileSystem.exists(new Path("/test/new/newdir/file")));    
  }
  
  public void testRenameDirectoryMoveToNonExistentDirectory() throws Exception {
    Path src = new Path("/test/hadoop/dir");
    s3FileSystem.mkdirs(src);
    Path dst = new Path("/test/new/newdir");
    rename(src, dst, false, true, false);
  }
  
  public void testRenameDirectoryMoveToExistingDirectory() throws Exception {
    Path src = new Path("/test/hadoop/dir");
    s3FileSystem.mkdirs(src);
    createEmptyFile(new Path("/test/hadoop/dir/file1"));
    createEmptyFile(new Path("/test/hadoop/dir/subdir/file2"));
    
    Path dst = new Path("/test/new/newdir");
    s3FileSystem.mkdirs(dst.getParent());
    rename(src, dst, true, false, true);
    
    assertFalse("Nested file1 exists", s3FileSystem.exists(new Path("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists", s3FileSystem.exists(new Path("/test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists", s3FileSystem.exists(new Path("/test/new/newdir/file1")));
    assertTrue("Renamed nested exists", s3FileSystem.exists(new Path("/test/new/newdir/subdir/file2")));
  }
  
  public void testRenameDirectoryAsExistingFile() throws Exception {
    Path src = new Path("/test/hadoop/dir");
    s3FileSystem.mkdirs(src);
    Path dst = new Path("/test/new/newfile");
    createEmptyFile(dst);
    rename(src, dst, false, true, true);
  }
  
  public void testRenameDirectoryAsExistingDirectory() throws Exception {
    Path src = new Path("/test/hadoop/dir");
    s3FileSystem.mkdirs(src);
    createEmptyFile(new Path("/test/hadoop/dir/file1"));
    createEmptyFile(new Path("/test/hadoop/dir/subdir/file2"));
    
    Path dst = new Path("/test/new/newdir");
    s3FileSystem.mkdirs(dst);
    rename(src, dst, true, false, true);
    assertTrue("Destination changed", s3FileSystem.exists(new Path("/test/new/newdir/dir")));    
    assertFalse("Nested file1 exists", s3FileSystem.exists(new Path("/test/hadoop/dir/file1")));
    assertFalse("Nested file2 exists", s3FileSystem.exists(new Path("/test/hadoop/dir/subdir/file2")));
    assertTrue("Renamed nested file1 exists", s3FileSystem.exists(new Path("/test/new/newdir/dir/file1")));
    assertTrue("Renamed nested exists", s3FileSystem.exists(new Path("/test/new/newdir/dir/subdir/file2")));
  }
  
  private void rename(Path src, Path dst, boolean renameSucceeded, boolean srcExists, boolean dstExists) throws IOException {
    assertEquals("Rename result", renameSucceeded, s3FileSystem.rename(src, dst));
    assertEquals("Source exists", srcExists, s3FileSystem.exists(src));
    assertEquals("Destination exists", dstExists, s3FileSystem.exists(dst));
  }

  private void createEmptyFile(Path path) throws IOException {
    FSDataOutputStream out = s3FileSystem.create(path, false,
                                                 s3FileSystem.getConf().getInt("io.file.buffer.size", 4096),
                                                 (short) 1, BLOCK_SIZE);
    out.write(data, 0, BLOCK_SIZE);
    out.close();
  }

}
