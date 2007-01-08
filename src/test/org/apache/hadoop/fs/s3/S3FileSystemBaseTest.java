package org.apache.hadoop.fs.s3;

import java.io.IOException;
import java.net.URI;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FSOutputStream;
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
    assertNull(s3FileSystem.listPathsRaw(testDirs[0]));

    for (Path path : testDirs) {
      assertTrue(s3FileSystem.mkdirs(path));
    }

    Path[] paths = s3FileSystem.listPathsRaw(new Path("/"));

    assertEquals(1, paths.length);
    assertEquals(new Path("/test"), paths[0]);

    paths = s3FileSystem.listPathsRaw(new Path("/test"));
    assertEquals(1, paths.length);
    assertEquals(new Path("/test/hadoop"), paths[0]);

    paths = s3FileSystem.listPathsRaw(new Path("/test/hadoop"));
    assertEquals(3, paths.length);
    assertEquals(new Path("/test/hadoop/a"), paths[0]);
    assertEquals(new Path("/test/hadoop/b"), paths[1]);
    assertEquals(new Path("/test/hadoop/c"), paths[2]);

    paths = s3FileSystem.listPathsRaw(new Path("/test/hadoop/a"));
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

    FSOutputStream out = s3FileSystem.createRaw(path, false, (short) 1, BLOCK_SIZE);
    out.write(data, 0, len);
    out.close();

    assertTrue("Exists", s3FileSystem.exists(path));
    
    assertEquals("Block size", Math.min(len, BLOCK_SIZE), s3FileSystem.getBlockSize(path));

    assertEquals("Length", len, s3FileSystem.getLength(path));

    FSInputStream in = s3FileSystem.openRaw(path);
    byte[] buf = new byte[len];

    in.readFully(0, buf);

    assertEquals(len, buf.length);
    for (int i = 0; i < buf.length; i++) {
      assertEquals("Position " + i, data[i], buf[i]);
    }
    
    assertTrue("Deleted", s3FileSystem.deleteRaw(path));
    
    assertFalse("No longer exists", s3FileSystem.exists(path));

  }

  public void testOverwrite() throws IOException {
    Path path = new Path("/test/hadoop/file");
    
    s3FileSystem.mkdirs(path.getParent());

    FSOutputStream out = s3FileSystem.createRaw(path, false, (short) 1, BLOCK_SIZE);
    out.write(data, 0, BLOCK_SIZE);
    out.close();
    
    assertTrue("Exists", s3FileSystem.exists(path));
    assertEquals("Length", BLOCK_SIZE, s3FileSystem.getLength(path));
    
    try {
      s3FileSystem.createRaw(path, false, (short) 1, 128);
      fail("Should throw IOException.");
    } catch (IOException e) {
      // Expected
    }
    
    out = s3FileSystem.createRaw(path, true, (short) 1, BLOCK_SIZE);
    out.write(data, 0, BLOCK_SIZE / 2);
    out.close();
    
    assertTrue("Exists", s3FileSystem.exists(path));
    assertEquals("Length", BLOCK_SIZE / 2, s3FileSystem.getLength(path));
    
  }

  public void testWriteInNonExistentDirectory() throws IOException {
    Path path = new Path("/test/hadoop/file");    
    FSOutputStream out = s3FileSystem.createRaw(path, false, (short) 1, BLOCK_SIZE);
    out.write(data, 0, BLOCK_SIZE);
    out.close();
    
    assertTrue("Exists", s3FileSystem.exists(path));
    assertEquals("Length", BLOCK_SIZE, s3FileSystem.getLength(path));
    assertTrue("Parent exists", s3FileSystem.exists(path.getParent()));
  }


  public void testRename() throws Exception {
    int len = BLOCK_SIZE;
    
    Path path = new Path("/test/hadoop/file");
    
    s3FileSystem.mkdirs(path.getParent());

    FSOutputStream out = s3FileSystem.createRaw(path, false, (short) 1, BLOCK_SIZE);
    out.write(data, 0, len);
    out.close();

    assertTrue("Exists", s3FileSystem.exists(path));

    Path newPath = new Path("/test/hadoop/newfile");
    s3FileSystem.rename(path, newPath);
    assertFalse("No longer exists", s3FileSystem.exists(path));
    assertTrue("Moved", s3FileSystem.exists(newPath));

    FSInputStream in = s3FileSystem.openRaw(newPath);
    byte[] buf = new byte[len];
    
    in.readFully(0, buf);

    assertEquals(len, buf.length);
    for (int i = 0; i < buf.length; i++) {
      assertEquals("Position " + i, data[i], buf[i]);
    }
  }


}
