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

package org.apache.hadoop.fs.slive;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class which generates a file or directory path using a simple random
 * generation algorithm stated in http://issues.apache.org/jira/browse/HDFS-708
 */
class PathFinder {

  private static final Logger LOG = LoggerFactory.getLogger(PathFinder.class);

  private enum Type {
    FILE, DIRECTORY
  }

  private static final String DIR_PREFIX = "sl_dir_";
  private static final String FILE_PREFIX = "sl_file_";

  private Path basePath;
  private ConfigExtractor config;
  private Random rnd;
  
  // Used to store scanned existing paths
  private List<Path> existingFiles;
  private List<Path> existingDirs;

  PathFinder(ConfigExtractor cfg, Random rnd) {
    this.basePath = cfg.getDataPath();
    this.config = cfg;
    this.rnd = rnd;
    this.existingFiles = new ArrayList<>();
    this.existingDirs = new ArrayList<>();
  }

  /**
   * Scan all paths under base_dir and record existing files and directories
   */
  private void scanBaseDirectory() {
    try {
      FileSystem fs = basePath.getFileSystem(config.getConfig());
      LOG.info("Starting to scan base_dir: " + basePath);
      // Clear existing lists
      clearExistingPaths();
      
      // Recursively scan directories
      scanDirectoryRecursively(fs, basePath);
      
      // Print summary only (avoid huge log output)
      LOG.info("Scan complete: found " + existingFiles.size() + " files, " 
          + existingDirs.size() + " directories");
      
    } catch (IOException e) {
      LOG.error("Error scanning base_dir: " + e.getMessage(), e);
      clearExistingPaths();
    }
  }
  
  private void clearExistingPaths() {
    existingFiles.clear();
    existingDirs.clear();
  }

  /**
   * Recursively scan directories
   */
  private void scanDirectoryRecursively(FileSystem fs, Path dir) throws IOException {
    if (!fs.exists(dir)) {
      return;
    }
    
    FileStatus[] statuses = fs.listStatus(dir);
    if (statuses == null || statuses.length == 0) {
      return;
    }
    
    for (FileStatus status : statuses) {
      Path path = status.getPath();
      if (status.isFile()) {
        existingFiles.add(path);
      } else if (status.isDirectory()) {
        existingDirs.add(path);
        // Recursively scan subdirectories
        scanDirectoryRecursively(fs, path);
      }
    }
  }

  /**
   * Randomly select one from existing files
   */
  private Path getExistingFile() {
    if (existingFiles.isEmpty()) {
      throw new RuntimeException("No files found in base_dir, cannot perform read/delete operations");
    }
    int index = rnd.nextInt(existingFiles.size());
    Path selectedFile = existingFiles.get(index);
    LOG.info("Selected from existing files: " + selectedFile);
    return selectedFile;
  }

  /**
   * Randomly select one from existing directories
   */
  private Path getExistingDirectory() {
    if (existingDirs.isEmpty()) {
      throw new RuntimeException("No directories found in base_dir, cannot perform ls operations");
    }
    int index = rnd.nextInt(existingDirs.size());
    Path selectedDir = existingDirs.get(index);
    LOG.info("Selected from existing directories: " + selectedDir);
    return selectedDir;
  }

  /**
   * This function uses a simple recursive algorithm to generate a path name
   * using the current id % limitPerDir and using current id / limitPerDir to
   * form the rest of the tree segments
   * 
   * @param curId
   *          the current id to use for determining the current directory id %
   *          per directory limit and then used for determining the next segment
   *          of the path to use, if <= zero this will return the base path
   * @param limitPerDir
   *          the per directory file limit used in modulo and division
   *          operations to calculate the file name and path tree
   * @param type
   *          directory or file enumeration
   * @return Path
   */
  private Path getPath(int curId, int limitPerDir, Type type) {
    return getPath(curId, limitPerDir, type, null);
  }
  
  private Path getPath(int curId, int limitPerDir, Type type, String suffix) {
    if (curId <= 0) {
      return basePath;
    }
    String name = "";
    switch (type) {
    case FILE:
      name = FILE_PREFIX + new Integer(curId % limitPerDir).toString();
      break;
    case DIRECTORY:
      name = DIR_PREFIX + new Integer(curId % limitPerDir).toString();
      break;
    }
    if (suffix != null) {
      name += "_" + suffix;
    }
    Path base = getPath((curId / limitPerDir), limitPerDir, Type.DIRECTORY);
    return new Path(base, name);
  }

  /**
   * Gets a file path using the given configuration provided total files and
   * files per directory
   * 
   * @return path
   */
  Path getFile() {
    return getFile(null);
  }

  /**
   * Gets a file path based on operation type and configuration
   * 
   * @param operationType the type of operation (can be null for backward compatibility)
   * @return path
   */
  Path getFile(String operationType) {
    boolean useNewAlgorithm = config.shouldUseNewAlgorithm();
    
    // Handle operations that need existing files
    if (isExistingFileOperation(operationType)) {
      if (useNewAlgorithm) {
        LOG.info("Use new algorithm mode: scanning base_dir for " + operationType + " operation");
        scanBaseDirectory();
        return getExistingFile();
      }
      // Fall through to original algorithm for normal mode
    }
    
    // Handle CREATE operation
    if ("CREATE".equals(operationType)) {
      if (useNewAlgorithm) {
        LOG.info("Generating unique path for CREATE operation");
        return generateUniquePath();
      }
      // Fall through to original algorithm for normal mode
    }
    
    // Use original algorithm for all other cases
    LOG.info("Using original algorithm for " + (operationType != null ? operationType : "default") + " operation");
    return generateOriginalPath();
  }
  
  private boolean isExistingFileOperation(String operationType) {
    return "READ".equals(operationType) || "DELETE".equals(operationType) || 
           "TRUNCATE".equals(operationType) || "APPEND".equals(operationType) || 
           "RENAME_SRC".equals(operationType);
  }
  
  private Path generateUniquePath() {
    int fileLimit = config.getTotalFiles();
    int dirLimit = config.getDirSize();
    int startPoint = 1 + rnd.nextInt(fileLimit);
    String uniqueId = UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    return getPath(startPoint, dirLimit, Type.FILE, uniqueId);
  }
  
  private Path generateOriginalPath() {
    int fileLimit = config.getTotalFiles();
    int dirLimit = config.getDirSize();
    int startPoint = 1 + rnd.nextInt(fileLimit);
    return getPath(startPoint, dirLimit, Type.FILE);
  }

  /**
   * Gets a directory path using the given configuration provided total files
   * and files per directory
   * 
   * @return path
   */
  Path getDirectory() {
    return getDirectory(null);
  }

  /**
   * Gets a directory path based on operation type
   * For CREATE/MKDIR operations: use original algorithm (write to base_dir)
   * For LS operations: scan base_dir and select from existing directories
   * 
   * @param operationType the type of operation (can be null for backward compatibility)
   * @return path
   */
  Path getDirectory(String operationType) { 
    boolean useNewAlgorithm = config.shouldUseNewAlgorithm();   
    // For LS operation, scan base_dir and select existing directories each time
    if ("LS".equals(operationType)) {
      if (useNewAlgorithm) {
        LOG.info("Starting to scan base_dir and select existing directories for LS operation");
        scanBaseDirectory();
        return getExistingDirectory();
      }
      // Fall through to original algorithm for normal mode
    }

    // Use original algorithm by default
    int fileLimit = config.getTotalFiles();
    int dirLimit = config.getDirSize();
    int startPoint = rnd.nextInt(fileLimit);
    return getPath(startPoint, dirLimit, Type.DIRECTORY);
  }

}
