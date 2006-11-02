/* Copyright 2005 The Apache Software Foundation
 *
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
package org.apache.hadoop.filecache;

import org.apache.commons.logging.*;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.fs.*;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.net.URI;

/*******************************************************************************
 * The DistributedCache maintains all the caching information of cached archives
 * and unarchives all the files as well and returns the path
 * 
 * @author Mahadev Konar
 ******************************************************************************/
public class DistributedCache {
  // cacheID to cacheStatus mapping
  private static TreeMap cachedArchives = new TreeMap();
  // buffer size for reading checksum files
  private static final int CRC_BUFFER_SIZE = 64 * 1024;
  
  /**
   * 
   * @param cache the cache to be localized, this should be specified as 
   * new URI(dfs://hostname:port/absoulte_path_to_file#LINKNAME). If no schema 
   * or hostname:port is provided the file is assumed to be in the filesystem
   * being used in the Configuration
   * @param conf The Confguration file which contains the filesystem
   * @param baseDir The base cache Dir where you wnat to localize the files/archives
   * @param isArchive if the cache is an archive or a file. In case it is an archive
   *  with a .zip or .jar extension it will be unzipped/unjarred automatically 
   *  and the directory where the archive is unjarred is returned as the Path.
   *  In case of a file, the path to the file is returned
   * @param md5 this is a mere checksum to verufy if you are using the right cache. 
   * You need to pass the md5 of the crc file in DFS. This is matched against the one
   * calculated in this api and if it does not match, the cache is not localized.
   * @param currentWorkDir this is the directory where you would want to create symlinks 
   * for the locally cached files/archives
   * @return the path to directory where the archives are unjarred in case of archives,
   * the path to the file where the file is copied locally 
   * @throws IOException
   */
  public static Path getLocalCache(URI cache, Configuration conf, Path baseDir,
      boolean isArchive, String md5, Path currentWorkDir) throws IOException {
    String cacheId = makeRelative(cache, conf);
    CacheStatus lcacheStatus;
    Path localizedPath;
    synchronized (cachedArchives) {
      if (!cachedArchives.containsKey(cacheId)) {
        // was never localized
        lcacheStatus = new CacheStatus();
        lcacheStatus.currentStatus = false;
        lcacheStatus.refcount = 1;
        lcacheStatus.localLoadPath = new Path(baseDir, new Path(cacheId));
        cachedArchives.put(cacheId, lcacheStatus);
      } else {
        lcacheStatus = (CacheStatus) cachedArchives.get(cacheId);
        synchronized (lcacheStatus) {
          lcacheStatus.refcount++;
        }
      }
    }
    synchronized (lcacheStatus) {
      localizedPath = localizeCache(cache, lcacheStatus, conf, isArchive, md5, currentWorkDir);
    }
    // try deleting stuff if you can
    long size = FileUtil.getDU(new File(baseDir.toString()));
    // setting the cache size to a default of 1MB
    long allowedSize = conf.getLong("local.cache.size", 1048576L);
    if (allowedSize < size) {
      // try some cache deletions
      deleteCache(conf);
    }
    return localizedPath;
  }
  
  /**
   * This is the opposite of getlocalcache. When you are done with
   * using the cache, you need to release the cache
   * @param cache The cache URI to be released
   * @param conf configuration which contains the filesystem the cache 
   * is contained in.
   * @throws IOException
   */
  public static void releaseCache(URI cache, Configuration conf)
      throws IOException {
    String cacheId = makeRelative(cache, conf);
    synchronized (cachedArchives) {
      CacheStatus lcacheStatus = (CacheStatus) cachedArchives.get(cacheId);
      synchronized (lcacheStatus) {
        lcacheStatus.refcount--;
      }
    }
  }
  
  // To delete the caches which have a refcount of zero
  
  private static void deleteCache(Configuration conf) throws IOException {
    // try deleting cache Status with refcount of zero
    synchronized (cachedArchives) {
      for (Iterator it = cachedArchives.keySet().iterator(); it.hasNext();) {
        String cacheId = (String) it.next();
        CacheStatus lcacheStatus = (CacheStatus) cachedArchives.get(cacheId);
        if (lcacheStatus.refcount == 0) {
          // delete this cache entry
          FileSystem.getNamed("local", conf).delete(lcacheStatus.localLoadPath);
          it.remove();
        }
      }
    }
  }

  /*
   * Returns the relative path of the dir this cache will be localized in
   * relative path that this cache will be localized in. For
   * dfs://hostname:port/absolute_path -- the relative path is
   * hostname/absolute path -- if it is just /absolute_path -- then the
   * relative path is hostname of DFS this mapred cluster is running
   * on/absolute_path
   */
  private static String makeRelative(URI cache, Configuration conf)
      throws IOException {
    String fsname = cache.getScheme();
    String path;
    FileSystem dfs = FileSystem.get(conf);
    if ("dfs".equals(fsname)) {
      path = cache.getHost() + cache.getPath();
    } else {
      String[] split = dfs.getName().split(":");
      path = split[0] + cache.getPath();
    }
    return path;
  }

  private static Path cacheFilePath(Path p) {
    return new Path(p, p.getName());
  }

  // the methoed which actually copies the caches locally and unjars/unzips them
  private static Path localizeCache(URI cache, CacheStatus cacheStatus,
      Configuration conf, boolean isArchive, String md5, Path currentWorkDir) throws IOException {
    boolean b = true;
    boolean doSymlink = getSymlink(conf);
    FileSystem dfs = getFileSystem(cache, conf);
    b = ifExistsAndFresh(cacheStatus, cache, dfs, md5, conf);
    String link = currentWorkDir.toString() + Path.SEPARATOR + cache.getFragment();
    File flink = new File(link);
    if (b) {
      if (isArchive) {
        if (doSymlink){
          if (!flink.exists())
            FileUtil.symLink(cacheStatus.localLoadPath.toString(), 
                link);
        }
        return cacheStatus.localLoadPath;
      }
      else {
        if (doSymlink){
          if (!flink.exists())
            FileUtil.symLink(cacheFilePath(cacheStatus.localLoadPath).toString(), 
              link);
        }
        return cacheFilePath(cacheStatus.localLoadPath);
      }
    } else {
      // remove the old archive
      // if the old archive cannot be removed since it is being used by another
      // job
      // return null
      if (cacheStatus.refcount > 1 && (cacheStatus.currentStatus == true))
        throw new IOException("Cache " + cacheStatus.localLoadPath.toString()
            + " is in use and cannot be refreshed");
      byte[] checkSum = createMD5(cache, conf);
      FileSystem localFs = FileSystem.getNamed("local", conf);
      localFs.delete(cacheStatus.localLoadPath);
      Path parchive = new Path(cacheStatus.localLoadPath,
                               new Path(cacheStatus.localLoadPath.getName()));
      if (!localFs.mkdirs(cacheStatus.localLoadPath)) {
          throw new IOException("Mkdirs failed to create directory " + 
                                cacheStatus.localLoadPath.toString());
      }
      String cacheId = cache.getPath();
      dfs.copyToLocalFile(new Path(cacheId), parchive);
      dfs.copyToLocalFile(new Path(cacheId + "_md5"), new Path(parchive
          .toString()
          + "_md5"));
      if (isArchive) {
        String tmpArchive = parchive.toString().toLowerCase();
        if (tmpArchive.endsWith(".jar")) {
          RunJar.unJar(new File(parchive.toString()), new File(parchive
              .getParent().toString()));
        } else if (tmpArchive.endsWith(".zip")) {
          FileUtil.unZip(new File(parchive.toString()), new File(parchive
              .getParent().toString()));

        }
        // else will not do anyhting
        // and copy the file into the dir as it is
      }
      // create a symlink if #NAME is specified as fragment in the
      // symlink
      cacheStatus.currentStatus = true;
      cacheStatus.md5 = checkSum;
    }
    
    if (isArchive){
      if (doSymlink){
        if (!flink.exists())
          FileUtil.symLink(cacheStatus.localLoadPath.toString(), 
            link);
      }
      return cacheStatus.localLoadPath;
    }
    else {
      if (doSymlink){
        if (!flink.exists())
          FileUtil.symLink(cacheFilePath(cacheStatus.localLoadPath).toString(), 
            link);
      }
      return cacheFilePath(cacheStatus.localLoadPath);
    }
  }

  // Checks if the cache has already been localized and is fresh
  private static boolean ifExistsAndFresh(CacheStatus lcacheStatus, URI cache,
      FileSystem dfs, String confMD5, Configuration conf) throws IOException {
    // compute the md5 of the crc
    byte[] digest = null;
    byte[] fsDigest = createMD5(cache, conf);
    byte[] confDigest = StringUtils.hexStringToByte(confMD5);
    // check for existence of the cache
    if (lcacheStatus.currentStatus == false) {
      return false;
    } else {
      digest = lcacheStatus.md5;
      if (!MessageDigest.isEqual(confDigest, fsDigest)) {
        throw new IOException("Inconsistencty in data caching, "
            + "Cache archives have been changed");
      } else {
        if (!MessageDigest.isEqual(confDigest, digest)) {
          // needs refreshing
          return false;
        } else {
          // does not need any refreshing
          return true;
        }
      }
    }
  }

  /**
   * Returns md5 of the checksum file for a given dfs file.
   * This method also creates file filename_md5 existence of which
   * signifies a new cache has been loaded into dfs. So if you want to
   * refresh the cache, you need to delete this md5 file as well.
   * @param cache The cache to get the md5 checksum for
   * @param conf configuration
   * @return md5 of the crc of the cache parameter
   * @throws IOException
   */
  public static byte[] createMD5(URI cache, Configuration conf)
      throws IOException {
    byte[] b = new byte[CRC_BUFFER_SIZE];
    byte[] digest = null;

    FileSystem fileSystem = getFileSystem(cache, conf);
    String filename = cache.getPath();
    Path filePath = new Path(filename);
    Path md5File = new Path(filePath.getParent().toString() + Path.SEPARATOR
        + filePath.getName() + "_md5");
    MessageDigest md5 = null;
    try {
      md5 = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException na) {
      // do nothing
    }
    if (!fileSystem.exists(md5File)) {
      FSInputStream fsStream = fileSystem.openRaw(FileSystem
          .getChecksumFile(filePath));
      int read = fsStream.read(b);
      while (read != -1) {
        md5.update(b, 0, read);
        read = fsStream.read(b);
      }
      fsStream.close();
      digest = md5.digest();

      FSDataOutputStream out = fileSystem.create(md5File);
      out.write(digest);
      out.close();
    } else {
      FSInputStream fsStream = fileSystem.openRaw(md5File);
      digest = new byte[md5.getDigestLength()];
      // assuming reading 16 bytes once is not a problem
      // though it should be checked if 16 bytes have been read or not
      int read = fsStream.read(digest);
      fsStream.close();
    }

    return digest;
  }

  private static String getFileSysName(URI url) {
    String fsname = url.getScheme();
    if ("dfs".equals(fsname)) {
      String host = url.getHost();
      int port = url.getPort();
      return (port == (-1)) ? host : (host + ":" + port);
    } else {
      return null;
    }
  }

  private static FileSystem getFileSystem(URI cache, Configuration conf)
      throws IOException {
    String fileSysName = getFileSysName(cache);
    if (fileSysName != null)
      return FileSystem.getNamed(fileSysName, conf);
    else
      return FileSystem.get(conf);
  }

  /**
   * Set the configuration with the given set of archives
   * @param archives The list of archives that need to be localized
   * @param conf Configuration which will be changed
   */
  public static void setCacheArchives(URI[] archives, Configuration conf) {
    String sarchives = StringUtils.uriToString(archives);
    conf.set("mapred.cache.archives", sarchives);
  }

  /**
   * Set the configuration with the given set of files
   * @param files The list of files that need to be localized
   * @param conf Configuration which will be changed
   */
  public static void setCacheFiles(URI[] files, Configuration conf) {
    String sfiles = StringUtils.uriToString(files);
    conf.set("mapred.cache.files", sfiles);
  }

  /**
   * Get cache archives set in the Configuration
   * @param conf The configuration which contains the archives
   * @return A URI array of the caches set in the Configuration
   * @throws IOException
   */
  public static URI[] getCacheArchives(Configuration conf) throws IOException {
    return StringUtils.stringToURI(conf.getStrings("mapred.cache.archives"));
  }

  /**
   * Get cache files set in the Configuration
   * @param conf The configuration which contains the files
   * @return A URI array of the files set in the Configuration
   * @throws IOException
   */

  public static URI[] getCacheFiles(Configuration conf) throws IOException {
    return StringUtils.stringToURI(conf.getStrings("mapred.cache.files"));
  }

  /**
   * Return the path array of the localized caches
   * @param conf Configuration that contains the localized archives
   * @return A path array of localized caches
   * @throws IOException
   */
  public static Path[] getLocalCacheArchives(Configuration conf)
      throws IOException {
    return StringUtils.stringToPath(conf
        .getStrings("mapred.cache.localArchives"));
  }

  /**
   * Return the path array of the localized files
   * @param conf Configuration that contains the localized files
   * @return A path array of localized files
   * @throws IOException
   */
  public static Path[] getLocalCacheFiles(Configuration conf)
      throws IOException {
    return StringUtils.stringToPath(conf.getStrings("mapred.cache.localFiles"));
  }

  /**
   * Get the md5 checksums of the archives
   * @param conf The configuration which stored the md5's
   * @return a string array of md5 checksums 
   * @throws IOException
   */
  public static String[] getArchiveMd5(Configuration conf) throws IOException {
    return conf.getStrings("mapred.cache.archivemd5");
  }


  /**
   * Get the md5 checksums of the files
   * @param conf The configuration which stored the md5's
   * @return a string array of md5 checksums 
   * @throws IOException
   */
  public static String[] getFileMd5(Configuration conf) throws IOException {
    return conf.getStrings("mapred.cache.filemd5");
  }

  /**
   * This is to check the md5 of the archives to be localized
   * @param conf Configuration which stores the md5's
   * @param md5 comma seperated list of md5 checksums of the .crc's of archives.
   * The order should be the same as the order in which the archives are added
   */
  public static void setArchiveMd5(Configuration conf, String md5) {
    conf.set("mapred.cache.archivemd5", md5);
  }

  /**
   * This is to check the md5 of the files to be localized
   * @param conf Configuration which stores the md5's
   * @param md5 comma seperated list of md5 checksums of the .crc's of files.
   * The order should be the same as the order in which the files are added
   */
  public static void setFileMd5(Configuration conf, String md5) {
    conf.set("mapred.cache.filemd5", md5);
  }
  
  /**
   * Set the conf to contain the location for localized archives 
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma seperated list of local archives
   */
  public static void setLocalArchives(Configuration conf, String str) {
    conf.set("mapred.cache.localArchives", str);
  }

  /**
   * Set the conf to contain the location for localized files 
   * @param conf The conf to modify to contain the localized caches
   * @param str a comma seperated list of local files
   */
  public static void setLocalFiles(Configuration conf, String str) {
    conf.set("mapred.cache.localFiles", str);
  }

  /**
   * Add a archives to be localized to the conf
   * @param uri The uri of the cache to be localized
   * @param conf Configuration to add the cache to
   */
  public static void addCacheArchive(URI uri, Configuration conf) {
    String archives = conf.get("mapred.cache.archives");
    conf.set("mapred.cache.archives", archives == null ? uri.toString()
        : archives + "," + uri.toString());
  }
  
  /**
   * Add a file to be localized to the conf
   * @param uri The uri of the cache to be localized
   * @param conf Configuration to add the cache to
   */
  public static void addCacheFile(URI uri, Configuration conf) {
    String files = conf.get("mapred.cache.files");
    conf.set("mapred.cache.files", files == null ? uri.toString() : files + ","
        + uri.toString());
  }
  
  /**
   * This method allows you to create symlinks in the current working directory
   * of the task to all the cache files/archives
   * @param conf the jobconf 
   */
  public static void createSymlink(Configuration conf){
    conf.set("mapred.create.symlink", "yes");
  }
  
  /**
   * This method checks to see if symlinks are to be create for the 
   * localized cache files in the current working directory 
   * @param conf the jobconf
   * @return true if symlinks are to be created- else return false
   */
  public static boolean getSymlink(Configuration conf){
    String result = conf.get("mapred.create.symlink");
    if ("yes".equals(result)){
      return true;
    }
    return false;
  }

  /**
   * This method checks if there is a conflict in the fragment names 
   * of the uris. Also makes sure that each uri has a fragment. It 
   * is only to be called if you want to create symlinks for 
   * the various archives and files.
   * @param uriFiles The uri array of urifiles
   * @param uriArchives the uri array of uri archives
   */
  public static boolean checkURIs(URI[]  uriFiles, URI[] uriArchives){
    if ((uriFiles == null) && (uriArchives == null)){
      return true;
    }
    if (uriFiles != null){
      for (int i = 0; i < uriFiles.length; i++){
        String frag1 = uriFiles[i].getFragment();
        if (frag1 == null)
          return false;
        for (int j=i+1; j < uriFiles.length; i++){
          String frag2 = uriFiles[j].getFragment();
          if (frag2 == null)
            return false;
          if (frag1.equalsIgnoreCase(frag2))
            return false;
        }
        if (uriArchives != null){
          for (int j = 0; j < uriArchives.length; j++){
            String frag2 = uriArchives[j].getFragment();
            if (frag2 == null){
              return false;
            }
            if (frag1.equalsIgnoreCase(frag2))
              return false;
            for (int k=j+1; k < uriArchives.length; k++){
              String frag3 = uriArchives[k].getFragment();
              if (frag3 == null)
                return false;
              if (frag2.equalsIgnoreCase(frag3))
                  return false;
            }
          }
        }
      }
    }
    return true;
  }

  private static class CacheStatus {
    // false, not loaded yet, true is loaded
    public boolean currentStatus;

    // the local load path of this cache
    public Path localLoadPath;

    // number of instances using this cache
    public int refcount;

    // The md5 checksum of the crc file of this cache
    public byte[] md5;
  }

}
