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

package org.apache.hadoop.yarn.util;

import static org.apache.hadoop.fs.CreateFlag.CREATE;
import static org.apache.hadoop.fs.CreateFlag.OVERWRITE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.apache.hadoop.yarn.api.records.URL;
import org.junit.Assert;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalDirAllocator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.factories.RecordFactory;
import org.apache.hadoop.yarn.factory.providers.RecordFactoryProvider;
import org.junit.AfterClass;
import org.junit.Test;

import org.apache.hadoop.thirdparty.com.google.common.cache.CacheBuilder;
import org.apache.hadoop.thirdparty.com.google.common.cache.CacheLoader;
import org.apache.hadoop.thirdparty.com.google.common.cache.LoadingCache;

/**
 * Unit test for the FSDownload class.
 */
public class TestFSDownload {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFSDownload.class);
  private static AtomicLong uniqueNumberGenerator =
    new AtomicLong(System.currentTimeMillis());
  private enum TEST_FILE_TYPE {
    TAR, JAR, ZIP, TGZ
  };
  private Configuration conf = new Configuration();

  @AfterClass
  public static void deleteTestDir() throws IOException {
    FileContext fs = FileContext.getLocalFSFileContext();
    fs.delete(new Path("target", TestFSDownload.class.getSimpleName()), true);
  }

  static final RecordFactory recordFactory =
    RecordFactoryProvider.getRecordFactory(null);

  static LocalResource createFile(FileContext files, Path p, int len,
      Random r, LocalResourceVisibility vis) throws IOException {
    createFile(files, p, len, r);
    LocalResource ret = recordFactory.newRecordInstance(LocalResource.class);
    ret.setResource(URL.fromPath(p));
    ret.setSize(len);
    ret.setType(LocalResourceType.FILE);
    ret.setVisibility(vis);
    ret.setTimestamp(files.getFileStatus(p).getModificationTime());
    return ret;
  }

  static void createFile(FileContext files, Path p, int len, Random r)
      throws IOException {
    FSDataOutputStream out = null;
    try {
      byte[] bytes = new byte[len];
      out = files.create(p, EnumSet.of(CREATE, OVERWRITE));
      r.nextBytes(bytes);
      out.write(bytes);
    } finally {
      if (out != null) out.close();
    }
  }

  static LocalResource createJar(FileContext files, Path p,
      LocalResourceVisibility vis) throws IOException {
    LOG.info("Create jar file " + p);
    File jarFile = new File((files.makeQualified(p)).toUri());
    FileOutputStream stream = new FileOutputStream(jarFile);
    LOG.info("Create jar out stream ");
    JarOutputStream out = new JarOutputStream(stream, new Manifest());
    ZipEntry entry = new ZipEntry("classes/1.class");
    out.putNextEntry(entry);
    out.write(1);
    out.write(2);
    out.write(3);
    out.closeEntry();
    ZipEntry entry2 = new ZipEntry("classes/2.class");
    out.putNextEntry(entry2);
    out.write(1);
    out.write(2);
    out.write(3);
    out.closeEntry();
    LOG.info("Done writing jar stream ");
    out.close();
    LocalResource ret = recordFactory.newRecordInstance(LocalResource.class);
    ret.setResource(URL.fromPath(p));
    FileStatus status = files.getFileStatus(p);
    ret.setSize(status.getLen());
    ret.setTimestamp(status.getModificationTime());
    ret.setType(LocalResourceType.PATTERN);
    ret.setVisibility(vis);
    ret.setPattern("classes/.*");
    return ret;
  }

  static LocalResource createTarFile(FileContext files, Path p, int len,
      Random r, LocalResourceVisibility vis) throws IOException,
      URISyntaxException {
    byte[] bytes = new byte[len];
    r.nextBytes(bytes);

    File archiveFile = new File(p.toUri().getPath() + ".tar");
    archiveFile.createNewFile();
    TarArchiveOutputStream out = new TarArchiveOutputStream(
        new FileOutputStream(archiveFile));
    TarArchiveEntry entry = new TarArchiveEntry(p.getName());
    entry.setSize(bytes.length);
    out.putArchiveEntry(entry);
    out.write(bytes);
    out.closeArchiveEntry();
    out.close();

    LocalResource ret = recordFactory.newRecordInstance(LocalResource.class);
    ret.setResource(URL.fromPath(new Path(p.toString()
        + ".tar")));
    ret.setSize(len);
    ret.setType(LocalResourceType.ARCHIVE);
    ret.setVisibility(vis);
    ret.setTimestamp(files.getFileStatus(new Path(p.toString() + ".tar"))
        .getModificationTime());
    return ret;
  }

  static LocalResource createTgzFile(FileContext files, Path p, int len,
      Random r, LocalResourceVisibility vis) throws IOException,
      URISyntaxException {
    byte[] bytes = new byte[len];
    r.nextBytes(bytes);

    File gzipFile = new File(p.toUri().getPath() + ".tar.gz");
    gzipFile.createNewFile();
    TarArchiveOutputStream out = new TarArchiveOutputStream(
        new GZIPOutputStream(new FileOutputStream(gzipFile)));
    TarArchiveEntry entry = new TarArchiveEntry(p.getName());
    entry.setSize(bytes.length);
    out.putArchiveEntry(entry);
    out.write(bytes);
    out.closeArchiveEntry();
    out.close();

    LocalResource ret = recordFactory.newRecordInstance(LocalResource.class);
    ret.setResource(URL.fromPath(new Path(p.toString()
        + ".tar.gz")));
    ret.setSize(len);
    ret.setType(LocalResourceType.ARCHIVE);
    ret.setVisibility(vis);
    ret.setTimestamp(files.getFileStatus(new Path(p.toString() + ".tar.gz"))
        .getModificationTime());
    return ret;
  }

  static LocalResource createJarFile(FileContext files, Path p, int len,
      Random r, LocalResourceVisibility vis) throws IOException,
      URISyntaxException {
    byte[] bytes = new byte[len];
    r.nextBytes(bytes);

    File archiveFile = new File(p.toUri().getPath() + ".jar");
    archiveFile.createNewFile();
    JarOutputStream out = new JarOutputStream(
        new FileOutputStream(archiveFile));
    out.putNextEntry(new JarEntry(p.getName()));
    out.write(bytes);
    out.closeEntry();
    out.close();

    LocalResource ret = recordFactory.newRecordInstance(LocalResource.class);
    ret.setResource(URL.fromPath(new Path(p.toString()
        + ".jar")));
    ret.setSize(len);
    ret.setType(LocalResourceType.ARCHIVE);
    ret.setVisibility(vis);
    ret.setTimestamp(files.getFileStatus(new Path(p.toString() + ".jar"))
        .getModificationTime());
    return ret;
  }

  static LocalResource createZipFile(FileContext files, Path p, int len,
      Random r, LocalResourceVisibility vis) throws IOException,
      URISyntaxException {
    byte[] bytes = new byte[len];
    r.nextBytes(bytes);

    File archiveFile = new File(p.toUri().getPath() + ".ZIP");
    archiveFile.createNewFile();
    ZipOutputStream out = new ZipOutputStream(
        new FileOutputStream(archiveFile));
    out.putNextEntry(new ZipEntry(p.getName()));
    out.write(bytes);
    out.closeEntry();
    out.close();

    LocalResource ret = recordFactory.newRecordInstance(LocalResource.class);
    ret.setResource(URL.fromPath(new Path(p.toString()
        + ".ZIP")));
    ret.setSize(len);
    ret.setType(LocalResourceType.ARCHIVE);
    ret.setVisibility(vis);
    ret.setTimestamp(files.getFileStatus(new Path(p.toString() + ".ZIP"))
        .getModificationTime());
    return ret;
  }

  @Test (timeout=10000)
  public void testDownloadBadPublic() throws IOException, URISyntaxException,
      InterruptedException {
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    FileContext files = FileContext.getLocalFSFileContext(conf);
    final Path basedir = files.makeQualified(new Path("target",
      TestFSDownload.class.getSimpleName()));
    files.mkdir(basedir, null, true);
    conf.setStrings(TestFSDownload.class.getName(), basedir.toString());
    
    Map<LocalResource, LocalResourceVisibility> rsrcVis =
        new HashMap<LocalResource, LocalResourceVisibility>();

    Random rand = new Random();
    long sharedSeed = rand.nextLong();
    rand.setSeed(sharedSeed);
    System.out.println("SEED: " + sharedSeed);

    Map<LocalResource,Future<Path>> pending =
      new HashMap<LocalResource,Future<Path>>();
    ExecutorService exec = HadoopExecutors.newSingleThreadExecutor();
    LocalDirAllocator dirs =
      new LocalDirAllocator(TestFSDownload.class.getName());
    int size = 512;
    LocalResourceVisibility vis = LocalResourceVisibility.PUBLIC;
    Path path = new Path(basedir, "test-file");
    LocalResource rsrc = createFile(files, path, size, rand, vis);
    rsrcVis.put(rsrc, vis);
    Path destPath = dirs.getLocalPathForWrite(
        basedir.toString(), size, conf);
    destPath = new Path (destPath,
      Long.toString(uniqueNumberGenerator.incrementAndGet()));
    FSDownload fsd =
      new FSDownload(files, UserGroupInformation.getCurrentUser(), conf,
          destPath, rsrc);
    pending.put(rsrc, exec.submit(fsd));
    exec.shutdown();
    while (!exec.awaitTermination(1000, TimeUnit.MILLISECONDS));
    Assert.assertTrue(pending.get(rsrc).isDone());

    try {
      for (Map.Entry<LocalResource,Future<Path>> p : pending.entrySet()) {
        p.getValue().get();
        Assert.fail("We localized a file that is not public.");
      }
    } catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof IOException);
    }
  }

  @Test (timeout=60000)
  public void testDownloadPublicWithStatCache() throws IOException,
      URISyntaxException, InterruptedException, ExecutionException {
    FileContext files = FileContext.getLocalFSFileContext(conf);
    Path basedir = files.makeQualified(new Path("target",
      TestFSDownload.class.getSimpleName()));

    // if test directory doesn't have ancestor permission, skip this test
    FileSystem f = basedir.getFileSystem(conf);
    assumeTrue(FSDownload.ancestorsHaveExecutePermissions(f, basedir, null));

    files.mkdir(basedir, null, true);
    conf.setStrings(TestFSDownload.class.getName(), basedir.toString());

    int size = 512;

    final ConcurrentMap<Path,AtomicInteger> counts =
        new ConcurrentHashMap<Path,AtomicInteger>();
    final CacheLoader<Path,Future<FileStatus>> loader =
        FSDownload.createStatusCacheLoader(conf);
    final LoadingCache<Path,Future<FileStatus>> statCache =
        CacheBuilder.newBuilder().build(new CacheLoader<Path,Future<FileStatus>>() {
      public Future<FileStatus> load(Path path) throws Exception {
        // increment the count
        AtomicInteger count = counts.get(path);
        if (count == null) {
          count = new AtomicInteger(0);
          AtomicInteger existing = counts.putIfAbsent(path, count);
          if (existing != null) {
            count = existing;
          }
        }
        count.incrementAndGet();

        // use the default loader
        return loader.load(path);
      }
    });

    // test FSDownload.isPublic() concurrently
    final int fileCount = 3;
    List<Callable<Boolean>> tasks = new ArrayList<Callable<Boolean>>();
    for (int i = 0; i < fileCount; i++) {
      Random rand = new Random();
      long sharedSeed = rand.nextLong();
      rand.setSeed(sharedSeed);
      System.out.println("SEED: " + sharedSeed);
      final Path path = new Path(basedir, "test-file-" + i);
      createFile(files, path, size, rand);
      final FileSystem fs = path.getFileSystem(conf);
      final FileStatus sStat = fs.getFileStatus(path);
      tasks.add(new Callable<Boolean>() {
        public Boolean call() throws IOException {
          return FSDownload.isPublic(fs, path, sStat, statCache);
        }
      });
    }

    ExecutorService exec = HadoopExecutors.newFixedThreadPool(fileCount);
    try {
      List<Future<Boolean>> futures = exec.invokeAll(tasks);
      // files should be public
      for (Future<Boolean> future: futures) {
        assertTrue(future.get());
      }
      // for each path exactly one file status call should be made
      for (AtomicInteger count: counts.values()) {
        assertSame(count.get(), 1);
      }
    } finally {
      exec.shutdown();
    }
  }

  @Test (timeout=10000)
  public void testDownload() throws IOException, URISyntaxException,
      InterruptedException {
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    FileContext files = FileContext.getLocalFSFileContext(conf);
    final Path basedir = files.makeQualified(new Path("target",
      TestFSDownload.class.getSimpleName()));
    files.mkdir(basedir, null, true);
    conf.setStrings(TestFSDownload.class.getName(), basedir.toString());
    
    Map<LocalResource, LocalResourceVisibility> rsrcVis =
        new HashMap<LocalResource, LocalResourceVisibility>();

    Random rand = new Random();
    long sharedSeed = rand.nextLong();
    rand.setSeed(sharedSeed);
    System.out.println("SEED: " + sharedSeed);

    Map<LocalResource,Future<Path>> pending =
      new HashMap<LocalResource,Future<Path>>();
    ExecutorService exec = HadoopExecutors.newSingleThreadExecutor();
    LocalDirAllocator dirs =
      new LocalDirAllocator(TestFSDownload.class.getName());
    int[] sizes = new int[10];
    for (int i = 0; i < 10; ++i) {
      sizes[i] = rand.nextInt(512) + 512;
      LocalResourceVisibility vis = LocalResourceVisibility.PRIVATE;
      if (i%2 == 1) {
        vis = LocalResourceVisibility.APPLICATION;
      }
      Path p = new Path(basedir, "" + i);
      LocalResource rsrc = createFile(files, p, sizes[i], rand, vis);
      rsrcVis.put(rsrc, vis);
      Path destPath = dirs.getLocalPathForWrite(
          basedir.toString(), sizes[i], conf);
      destPath = new Path (destPath,
          Long.toString(uniqueNumberGenerator.incrementAndGet()));
      FSDownload fsd =
          new FSDownload(files, UserGroupInformation.getCurrentUser(), conf,
              destPath, rsrc);
      pending.put(rsrc, exec.submit(fsd));
    }

    exec.shutdown();
    while (!exec.awaitTermination(1000, TimeUnit.MILLISECONDS));
    for (Future<Path> path: pending.values()) {
      Assert.assertTrue(path.isDone());
    }

    try {
      for (Map.Entry<LocalResource,Future<Path>> p : pending.entrySet()) {
        Path localized = p.getValue().get();
        assertEquals(sizes[Integer.parseInt(localized.getName())], p.getKey()
            .getSize());

        FileStatus status = files.getFileStatus(localized.getParent());
        FsPermission perm = status.getPermission();
        assertEquals("Cache directory permissions are incorrect",
            new FsPermission((short)0755), perm);

        status = files.getFileStatus(localized);
        perm = status.getPermission();
        System.out.println("File permission " + perm + 
            " for rsrc vis " + p.getKey().getVisibility().name());
        assert(rsrcVis.containsKey(p.getKey()));
        Assert.assertTrue("Private file should be 500",
            perm.toShort() == FSDownload.PRIVATE_FILE_PERMS.toShort());
      }
    } catch (ExecutionException e) {
      throw new IOException("Failed exec", e);
    }
  }

  private void downloadWithFileType(TEST_FILE_TYPE fileType) throws IOException, 
      URISyntaxException, InterruptedException{
    conf.set(CommonConfigurationKeys.FS_PERMISSIONS_UMASK_KEY, "077");
    FileContext files = FileContext.getLocalFSFileContext(conf);
    final Path basedir = files.makeQualified(new Path("target",
        TestFSDownload.class.getSimpleName()));
    files.mkdir(basedir, null, true);
    conf.setStrings(TestFSDownload.class.getName(), basedir.toString());

    Random rand = new Random();
    long sharedSeed = rand.nextLong();
    rand.setSeed(sharedSeed);
    System.out.println("SEED: " + sharedSeed);

    Map<LocalResource, Future<Path>> pending = new HashMap<LocalResource, Future<Path>>();
    ExecutorService exec = HadoopExecutors.newSingleThreadExecutor();
    LocalDirAllocator dirs = new LocalDirAllocator(
        TestFSDownload.class.getName());

    int size = rand.nextInt(512) + 512;
    LocalResourceVisibility vis = LocalResourceVisibility.PRIVATE;
    Path p = new Path(basedir, "" + 1);
    String strFileName = "";
    LocalResource rsrc = null;
    switch (fileType) {
    case TAR:
      rsrc = createTarFile(files, p, size, rand, vis);
      break;
    case JAR:
      rsrc = createJarFile(files, p, size, rand, vis);
      rsrc.setType(LocalResourceType.PATTERN);
      break;
    case ZIP:
      rsrc = createZipFile(files, p, size, rand, vis);
      strFileName = p.getName() + ".ZIP";
      break;
    case TGZ:
      rsrc = createTgzFile(files, p, size, rand, vis);
      break;
    }
    Path destPath = dirs.getLocalPathForWrite(basedir.toString(), size, conf);
    destPath = new Path (destPath,
        Long.toString(uniqueNumberGenerator.incrementAndGet()));
    FSDownload fsd = new FSDownload(files,
        UserGroupInformation.getCurrentUser(), conf, destPath, rsrc);
    pending.put(rsrc, exec.submit(fsd));
    exec.shutdown();
    while (!exec.awaitTermination(1000, TimeUnit.MILLISECONDS));
    try {
      pending.get(rsrc).get(); // see if there was an Exception during download
      FileStatus[] filesstatus = files.getDefaultFileSystem().listStatus(
          basedir);
      for (FileStatus filestatus : filesstatus) {
        if (filestatus.isDirectory()) {
          FileStatus[] childFiles = files.getDefaultFileSystem().listStatus(
              filestatus.getPath());
          for (FileStatus childfile : childFiles) {
            if(strFileName.endsWith(".ZIP") &&
               childfile.getPath().getName().equals(strFileName) &&
               !childfile.isDirectory()) {
               Assert.fail("Failure...After unzip, there should have been a" +
                 " directory formed with zip file name but found a file. "
                 + childfile.getPath());
            }
            if (childfile.getPath().getName().startsWith("tmp")) {
              Assert.fail("Tmp File should not have been there "
                  + childfile.getPath());
            }
          }
        }
      }
    }catch (Exception e) {
      throw new IOException("Failed exec", e);
    }
  }

  @Test (timeout=10000)
  public void testDownloadArchive() throws IOException, URISyntaxException,
      InterruptedException {
    downloadWithFileType(TEST_FILE_TYPE.TAR);
  }

  @Test (timeout=10000)
  public void testDownloadPatternJar() throws IOException, URISyntaxException,
      InterruptedException {
    downloadWithFileType(TEST_FILE_TYPE.JAR);
  }

  @Test (timeout=10000)
  public void testDownloadArchiveZip() throws IOException, URISyntaxException,
      InterruptedException {
    downloadWithFileType(TEST_FILE_TYPE.ZIP);
  }

  /*
   * To test fix for YARN-3029
   */
  @Test (timeout=10000)
  public void testDownloadArchiveZipWithTurkishLocale() throws IOException,
      URISyntaxException, InterruptedException {
    Locale defaultLocale = Locale.getDefault();
    // Set to Turkish
    Locale turkishLocale = new Locale("tr", "TR");
    Locale.setDefault(turkishLocale);
    downloadWithFileType(TEST_FILE_TYPE.ZIP);
    // Set the locale back to original default locale
    Locale.setDefault(defaultLocale);
  }

  @Test (timeout=10000)
  public void testDownloadArchiveTgz() throws IOException, URISyntaxException,
      InterruptedException {
    downloadWithFileType(TEST_FILE_TYPE.TGZ);
  }

  private void verifyPermsRecursively(FileSystem fs,
      FileContext files, Path p,
      LocalResourceVisibility vis) throws IOException {
    FileStatus status = files.getFileStatus(p);
    if (status.isDirectory()) {
      if (vis == LocalResourceVisibility.PUBLIC) {
        Assert.assertTrue(status.getPermission().toShort() ==
          FSDownload.PUBLIC_DIR_PERMS.toShort());
      }
      else {
        Assert.assertTrue(status.getPermission().toShort() ==
          FSDownload.PRIVATE_DIR_PERMS.toShort());
      }
      if (!status.isSymlink()) {
        FileStatus[] statuses = fs.listStatus(p);
        for (FileStatus stat : statuses) {
          verifyPermsRecursively(fs, files, stat.getPath(), vis);
        }
      }
    }
    else {
      if (vis == LocalResourceVisibility.PUBLIC) {
        Assert.assertTrue(status.getPermission().toShort() ==
          FSDownload.PUBLIC_FILE_PERMS.toShort());
      }
      else {
        Assert.assertTrue(status.getPermission().toShort() ==
          FSDownload.PRIVATE_FILE_PERMS.toShort());
      }
    }      
  }

  @Test (timeout=10000)
  public void testDirDownload() throws IOException, InterruptedException {
    FileContext files = FileContext.getLocalFSFileContext(conf);
    final Path basedir = files.makeQualified(new Path("target",
      TestFSDownload.class.getSimpleName()));
    files.mkdir(basedir, null, true);
    conf.setStrings(TestFSDownload.class.getName(), basedir.toString());
    
    Map<LocalResource, LocalResourceVisibility> rsrcVis =
        new HashMap<LocalResource, LocalResourceVisibility>();
  
    Random rand = new Random();
    long sharedSeed = rand.nextLong();
    rand.setSeed(sharedSeed);
    System.out.println("SEED: " + sharedSeed);

    Map<LocalResource,Future<Path>> pending =
      new HashMap<LocalResource,Future<Path>>();
    ExecutorService exec = HadoopExecutors.newSingleThreadExecutor();
    LocalDirAllocator dirs =
      new LocalDirAllocator(TestFSDownload.class.getName());
    for (int i = 0; i < 5; ++i) {
      LocalResourceVisibility vis = LocalResourceVisibility.PRIVATE;
      if (i%2 == 1) {
        vis = LocalResourceVisibility.APPLICATION;
      }

      Path p = new Path(basedir, "dir" + i + ".jar");
      LocalResource rsrc = createJar(files, p, vis);
      rsrcVis.put(rsrc, vis);
      Path destPath = dirs.getLocalPathForWrite(
          basedir.toString(), conf);
      destPath = new Path (destPath,
          Long.toString(uniqueNumberGenerator.incrementAndGet()));
      FSDownload fsd =
          new FSDownload(files, UserGroupInformation.getCurrentUser(), conf,
              destPath, rsrc);
      pending.put(rsrc, exec.submit(fsd));
    }

    exec.shutdown();
    while (!exec.awaitTermination(1000, TimeUnit.MILLISECONDS));
    for (Future<Path> path: pending.values()) {
      Assert.assertTrue(path.isDone());
    }

    try {
      
      for (Map.Entry<LocalResource,Future<Path>> p : pending.entrySet()) {
        Path localized = p.getValue().get();
        FileStatus status = files.getFileStatus(localized);

        System.out.println("Testing path " + localized);
        assert(status.isDirectory());
        assert(rsrcVis.containsKey(p.getKey()));
        
        verifyPermsRecursively(localized.getFileSystem(conf),
            files, localized, rsrcVis.get(p.getKey()));
      }
    } catch (ExecutionException e) {
      throw new IOException("Failed exec", e);
    }
  }

  @Test (timeout=10000)
  public void testUniqueDestinationPath() throws Exception {
    FileContext files = FileContext.getLocalFSFileContext(conf);
    final Path basedir = files.makeQualified(new Path("target",
        TestFSDownload.class.getSimpleName()));
    files.mkdir(basedir, null, true);
    conf.setStrings(TestFSDownload.class.getName(), basedir.toString());

    ExecutorService singleThreadedExec = HadoopExecutors
        .newSingleThreadExecutor();

    LocalDirAllocator dirs =
        new LocalDirAllocator(TestFSDownload.class.getName());
    Path destPath = dirs.getLocalPathForWrite(basedir.toString(), conf);
    destPath =
        new Path(destPath, Long.toString(uniqueNumberGenerator
            .incrementAndGet()));

    Path p = new Path(basedir, "dir" + 0 + ".jar");
    LocalResourceVisibility vis = LocalResourceVisibility.PRIVATE;
    LocalResource rsrc = createJar(files, p, vis);
    FSDownload fsd =
        new FSDownload(files, UserGroupInformation.getCurrentUser(), conf,
            destPath, rsrc);
    Future<Path> rPath = singleThreadedExec.submit(fsd);
    singleThreadedExec.shutdown();
    while (!singleThreadedExec.awaitTermination(1000, TimeUnit.MILLISECONDS));
    Assert.assertTrue(rPath.isDone());
    // Now FSDownload will not create a random directory to localize the
    // resource. Therefore the final localizedPath for the resource should be
    // destination directory (passed as an argument) + file name.
    Assert.assertEquals(destPath, rPath.get().getParent());
  }

  /**
   * This test method is responsible for creating an IOException resulting
   * from modification to the local resource's timestamp on the source FS just
   * before the download of this local resource has started.
   */
  @Test(timeout=10000)
  public void testResourceTimestampChangeDuringDownload()
      throws IOException, InterruptedException {
    conf = new Configuration();
    FileContext files = FileContext.getLocalFSFileContext(conf);
    final Path basedir = files.makeQualified(
        new Path("target", TestFSDownload.class.getSimpleName()));
    files.mkdir(basedir, null, true);
    conf.setStrings(TestFSDownload.class.getName(), basedir.toString());

    LocalDirAllocator dirs =
        new LocalDirAllocator(TestFSDownload.class.getName());

    Path path = new Path(basedir, "test-file");
    Random rand = new Random();
    long sharedSeed = rand.nextLong();
    rand.setSeed(sharedSeed);
    int size = 512;
    LocalResourceVisibility vis = LocalResourceVisibility.PUBLIC;
    LocalResource localResource = createFile(files, path, size, rand, vis);

    Path destPath = dirs.getLocalPathForWrite(basedir.toString(), size, conf);
    destPath = new Path(destPath,
        Long.toString(uniqueNumberGenerator.incrementAndGet()));

    FSDownload fsDownload = new FSDownload(files,
        UserGroupInformation.getCurrentUser(), conf, destPath, localResource);

    // Store the original local resource timestamp used to set up the
    // FSDownload object just before (but before the download starts)
    // for comparison purposes later on.
    long origLRTimestamp = localResource.getTimestamp();

    // Modify the local resource's timestamp to yesterday on the Filesystem
    // just before FSDownload starts.
    final long msInADay = 86400 * 1000;
    long modifiedFSTimestamp = origLRTimestamp - msInADay;
    try {
      Path sourceFsPath = localResource.getResource().toPath();
      FileSystem sourceFs = sourceFsPath.getFileSystem(conf);
      sourceFs.setTimes(sourceFsPath, modifiedFSTimestamp, modifiedFSTimestamp);
    } catch (URISyntaxException use) {
      Assert.fail("No exception expected.");
    }

    // Execute the FSDownload operation.
    Map<LocalResource, Future<Path>> pending = new HashMap<>();
    ExecutorService exec = HadoopExecutors.newSingleThreadExecutor();
    pending.put(localResource, exec.submit(fsDownload));

    exec.shutdown();

    exec.awaitTermination(1000, TimeUnit.MILLISECONDS);
    Assert.assertTrue(pending.get(localResource).isDone());

    try {
      for (Map.Entry<LocalResource, Future<Path>> p : pending.entrySet()) {
        p.getValue().get();
      }
      Assert.fail("Exception expected from timestamp update during download");
    } catch (ExecutionException ee) {
      Assert.assertTrue(ee.getCause() instanceof IOException);
      Assert.assertTrue("Exception contains original timestamp",
          ee.getMessage().contains(Times.formatISO8601(origLRTimestamp)));
      Assert.assertTrue("Exception contains modified timestamp",
          ee.getMessage().contains(Times.formatISO8601(modifiedFSTimestamp)));
    }
  }
}
