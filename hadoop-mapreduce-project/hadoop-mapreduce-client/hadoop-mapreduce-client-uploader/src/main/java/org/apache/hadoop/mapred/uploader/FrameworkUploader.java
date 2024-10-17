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

package org.apache.hadoop.mapred.uploader;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.AccessControlException;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NotLinkException;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

/**
 * Upload a MapReduce framework tarball to HDFS.
 * Usage:
 * sudo -u mapred mapred frameworkuploader -fs hdfs://`hostname`:8020 -target
 * /tmp/upload.tar.gz#mr-framework
*/
public class FrameworkUploader implements Runnable {
  private static final Pattern VAR_SUBBER =
      Pattern.compile(Shell.getEnvironmentVariableRegex());
  private static final Logger LOG =
      LoggerFactory.getLogger(FrameworkUploader.class);
  private Configuration conf = new Configuration();

  // Minimal required permissions for the uploaded framework
  private static final FsPermission FRAMEWORK_PERMISSION =
      new FsPermission(0644);

  @VisibleForTesting
  String input = null;
  @VisibleForTesting
  String whitelist = null;
  @VisibleForTesting
  String blacklist = null;
  @VisibleForTesting
  String target = null;
  @VisibleForTesting
  Path targetPath = null;
  @VisibleForTesting
  short initialReplication = 3;
  @VisibleForTesting
  short finalReplication = 10;
  @VisibleForTesting
  short acceptableReplication = 9;
  @VisibleForTesting
  int timeout = 10;
  private boolean ignoreSymlink = false;

  @VisibleForTesting
  Set<String> filteredInputFiles = new HashSet<>();
  @VisibleForTesting
  List<Pattern> whitelistedFiles = new LinkedList<>();
  @VisibleForTesting
  List<Pattern> blacklistedFiles = new LinkedList<>();

  private OutputStream targetStream = null;
  private FSDataOutputStream fsDataStream = null;
  private String alias = null;

  @VisibleForTesting
  void setConf(Configuration configuration) {
    conf = configuration;
  }

  private void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("mapred frameworkuploader", options);
  }

  public void run() {
    try {
      collectPackages();
      buildPackage();
      LOG.info("Uploaded " + target);
      System.out.println("Suggested mapreduce.application.framework.path " +
          target);
      LOG.info(
          "Suggested mapreduce.application.classpath $PWD/" + alias + "/*");
      System.out.println("Suggested classpath $PWD/" + alias + "/*");
    } catch (UploaderException|IOException|InterruptedException e) {
      LOG.error("Error in execution " + e.getMessage());
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @VisibleForTesting
  void collectPackages() throws UploaderException {
    parseLists();
    String[] list = StringUtils.split(input, File.pathSeparatorChar);
    for (String item : list) {
      LOG.info("Original source " + item);
      String expanded = expandEnvironmentVariables(item, System.getenv());
      LOG.info("Expanded source " + expanded);
      if (expanded.endsWith("*")) {
        File path = new File(expanded.substring(0, expanded.length() - 1));
        if (path.isDirectory()) {
          File[] files = path.listFiles();
          if (files != null) {
            for (File jar : files) {
              if (!jar.isDirectory()) {
                addJar(jar);
              } else {
                LOG.info("Ignored " + jar + " because it is a directory");
              }
            }
          } else {
            LOG.warn("Could not list directory " + path);
          }
        } else {
          LOG.warn("Ignored " + expanded + ". It is not a directory");
        }
      } else if (expanded.endsWith(".jar")) {
        File jarFile = new File(expanded);
        addJar(jarFile);
      } else if (!expanded.isEmpty()) {
        LOG.warn("Ignored " + expanded + " only jars are supported");
      }
    }
  }

  @VisibleForTesting
  void beginUpload() throws IOException, UploaderException {
    if (targetStream == null) {
      int lastIndex = target.indexOf('#');
      targetPath =
          new Path(
              target.substring(
                  0, lastIndex == -1 ? target.length() : lastIndex));
      alias = lastIndex != -1 ?
          target.substring(lastIndex + 1) :
          targetPath.getName();
      LOG.info("Target " + targetPath);
      FileSystem fileSystem = targetPath.getFileSystem(conf);

      targetStream = null;
      if (fileSystem instanceof DistributedFileSystem) {
        LOG.info("Set replication to " +
            initialReplication + " for path: " + targetPath);
        LOG.info("Disabling Erasure Coding for path: " + targetPath);
        DistributedFileSystem dfs = (DistributedFileSystem)fileSystem;
        DistributedFileSystem.HdfsDataOutputStreamBuilder builder =
            dfs.createFile(targetPath)
            .overwrite(true)
            .ecPolicyName(
                SystemErasureCodingPolicies.getReplicationPolicy().getName());
        if (initialReplication > 0) {
          builder.replication(initialReplication);
        }
        targetStream = builder.build();
      } else {
        LOG.warn("Cannot set replication to " +
            initialReplication + " for path: " + targetPath +
            " on a non-distributed filesystem " +
            fileSystem.getClass().getName());
      }
      if (targetStream == null) {
        targetStream = fileSystem.create(targetPath, true);
      }

      if (!FRAMEWORK_PERMISSION.equals(
          FRAMEWORK_PERMISSION.applyUMask(FsPermission.getUMask(conf)))) {
        LOG.info("Modifying permissions to " + FRAMEWORK_PERMISSION);
        fileSystem.setPermission(targetPath, FRAMEWORK_PERMISSION);
      }

      fsDataStream = (FSDataOutputStream) targetStream;
      if (targetPath.getName().endsWith("gz") ||
          targetPath.getName().endsWith("tgz")) {
        LOG.info("Creating GZip");
        targetStream = new GZIPOutputStream(targetStream);
      }

      Path current = targetPath.getParent();
      // Walk the path backwards to verify that the uploaded
      // framework is accessible for all users
      while (current != null) {
        try {
          FileStatus fstat = fileSystem.getFileStatus(current);
          FsPermission perm = fstat.getPermission();

          // Note: READ is not necessary to enter the directory.
          // We need to check only the EXECUTE flag
          boolean userCanEnter = perm.getUserAction()
              .implies(FsAction.EXECUTE);
          boolean groupCanEnter = perm.getGroupAction()
              .implies(FsAction.EXECUTE);
          boolean othersCanEnter = perm.getOtherAction()
              .implies(FsAction.EXECUTE);

          if (!userCanEnter || !groupCanEnter || !othersCanEnter) {
            LOG.warn("Path " + current + " is not accessible"
                + " for all users. Current permissions are: " + perm);
            LOG.warn("Please set EXECUTE permissions on this directory");
          }
          current = current.getParent();
        } catch (AccessControlException e) {
          LOG.warn("Path " + current + " is not accessible,"
              + " cannot retrieve permissions");
          LOG.warn("Please set EXECUTE permissions on this directory");
          LOG.debug("Stack trace", e);
          break;
        }
      }
    }
  }

  private long getSmallestReplicatedBlockCount()
      throws IOException {
    FileSystem fileSystem = targetPath.getFileSystem(conf);
    FileStatus status = fileSystem.getFileStatus(targetPath);
    long length = status.getLen();
    HashMap<Long, Integer> blockCount = new HashMap<>();

    // Start with 0s for each offset
    for (long offset = 0; offset < length; offset +=status.getBlockSize()) {
      blockCount.put(offset, 0);
    }

    // Count blocks
    BlockLocation[] locations = fileSystem.getFileBlockLocations(
        targetPath, 0, length);
    for(BlockLocation location: locations) {
      final int replicas = location.getHosts().length;
      blockCount.compute(
          location.getOffset(),
          (key, value) -> value == null ? 0 : value + replicas);
    }

    // Print out the results
    for (long offset = 0; offset < length; offset +=status.getBlockSize()) {
      LOG.info(String.format(
          "Replication counts offset:%d blocks:%d",
          offset, blockCount.get(offset)));
    }

    return Collections.min(blockCount.values());
  }

  private void endUpload()
      throws IOException, InterruptedException {
    FileSystem fileSystem = targetPath.getFileSystem(conf);
    if (fileSystem instanceof DistributedFileSystem) {
      fileSystem.setReplication(targetPath, finalReplication);
      LOG.info("Set replication to " +
          finalReplication + " for path: " + targetPath);
      if (timeout == 0) {
        LOG.info("Timeout is set to 0. Skipping replication check.");
      } else {
        long startTime = System.currentTimeMillis();
        long endTime = startTime;
        long currentReplication = 0;
        while(endTime - startTime < timeout * 1000 &&
             currentReplication < acceptableReplication) {
          Thread.sleep(1000);
          endTime = System.currentTimeMillis();
          currentReplication = getSmallestReplicatedBlockCount();
        }
        if (endTime - startTime >= timeout * 1000) {
          LOG.error(String.format(
              "Timed out after %d seconds while waiting for acceptable" +
                  " replication of %d (current replication is %d)",
              timeout, acceptableReplication, currentReplication));
        }
      }
    } else {
      LOG.info("Cannot set replication to " +
          finalReplication + " for path: " + targetPath +
          " on a non-distributed filesystem " +
          fileSystem.getClass().getName());
    }
  }

  @VisibleForTesting
  void buildPackage()
      throws IOException, UploaderException, InterruptedException {
    beginUpload();
    LOG.info("Compressing tarball");
    try (TarArchiveOutputStream out = new TarArchiveOutputStream(
        targetStream)) {
      for (String fullPath : filteredInputFiles) {
        LOG.info("Adding " + fullPath);
        File file = new File(fullPath);
        try (FileInputStream inputStream = new FileInputStream(file)) {
          ArchiveEntry entry = out.createArchiveEntry(file, file.getName());
          out.putArchiveEntry(entry);
          IOUtils.copyBytes(inputStream, out, 1024 * 1024);
          out.closeArchiveEntry();
        }
      }

      // Necessary to see proper replication counts in endUpload()
      fsDataStream.hflush();

      endUpload();
    } finally {
      if (targetStream != null) {
        targetStream.close();
      }
    }
  }

  private void parseLists() throws UploaderException {
    Map<String, String> env = System.getenv();
    for(Map.Entry<String, String> item : env.entrySet()) {
      LOG.info("Environment " + item.getKey() + " " + item.getValue());
    }
    String[] whiteListItems = StringUtils.split(whitelist);
    for (String pattern : whiteListItems) {
      String expandedPattern =
          expandEnvironmentVariables(pattern, env);
      Pattern compiledPattern =
          Pattern.compile("^" + expandedPattern + "$");
      LOG.info("Whitelisted " + compiledPattern.toString());
      whitelistedFiles.add(compiledPattern);
    }
    String[] blacklistItems = StringUtils.split(blacklist);
    for (String pattern : blacklistItems) {
      String expandedPattern =
          expandEnvironmentVariables(pattern, env);
      Pattern compiledPattern =
          Pattern.compile("^" + expandedPattern + "$");
      LOG.info("Blacklisted " + compiledPattern.toString());
      blacklistedFiles.add(compiledPattern);
    }
  }

  @VisibleForTesting
  String expandEnvironmentVariables(String innerInput, Map<String, String> env)
      throws UploaderException {
    boolean found;
    do {
      found = false;
      Matcher matcher = VAR_SUBBER.matcher(innerInput);
      StringBuffer stringBuffer = new StringBuffer();
      while (matcher.find()) {
        found = true;
        String var = matcher.group(1);
        // replace $env with the child's env constructed by tt's
        String replace = env.get(var);
        // the env key is not present anywhere .. simply set it
        if (replace == null) {
          throw new UploaderException("Environment variable does not exist " +
              var);
        }
        matcher.appendReplacement(
            stringBuffer, Matcher.quoteReplacement(replace));
      }
      matcher.appendTail(stringBuffer);
      innerInput = stringBuffer.toString();
    } while (found);
    return innerInput;
  }

  private void addJar(File jar) throws UploaderException{
    boolean found = false;
    if (!jar.getName().endsWith(".jar")) {
      LOG.info("Ignored non-jar " + jar.getAbsolutePath());
    }
    for (Pattern pattern : whitelistedFiles) {
      Matcher matcher = pattern.matcher(jar.getAbsolutePath());
      if (matcher.matches()) {
        LOG.info("Whitelisted " + jar.getAbsolutePath());
        found = true;
        break;
      }
    }
    boolean excluded = false;
    for (Pattern pattern : blacklistedFiles) {
      Matcher matcher = pattern.matcher(jar.getAbsolutePath());
      if (matcher.matches()) {
        LOG.info("Blacklisted " + jar.getAbsolutePath());
        excluded = true;
        break;
      }
    }
    if (ignoreSymlink && !excluded) {
      excluded = checkSymlink(jar);
    }
    if (found && !excluded) {
      LOG.info("Whitelisted " + jar.getAbsolutePath());
      if (!filteredInputFiles.add(jar.getAbsolutePath())) {
        throw new UploaderException("Duplicate jar" + jar.getAbsolutePath());
      }
    }
    if (!found) {
      LOG.info("Ignored " + jar.getAbsolutePath() + " because it is missing " +
          "from the whitelist");
    } else if (excluded) {
      LOG.info("Ignored " + jar.getAbsolutePath() + " because it is on " +
          "the the blacklist");
    }
  }

  /**
   * Check if the file is a symlink to the same directory.
   * @param jar The file to check
   * @return true, to ignore the directory
   */
  @VisibleForTesting
  boolean checkSymlink(File jar) {
    if (Files.isSymbolicLink(jar.toPath())) {
      try {
        java.nio.file.Path link = Files.readSymbolicLink(jar.toPath());
        java.nio.file.Path jarPath = Paths.get(jar.getAbsolutePath());
        String linkString = link.toString();
        java.nio.file.Path jarParent = jarPath.getParent();
        java.nio.file.Path linkPath =
            jarParent == null ? null : jarParent.resolve(linkString);
        java.nio.file.Path linkPathParent =
            linkPath == null ? null : linkPath.getParent();
        java.nio.file.Path normalizedLinkPath =
            linkPathParent == null ? null : linkPathParent.normalize();
        if (normalizedLinkPath != null && jarParent.normalize().equals(
            normalizedLinkPath)) {
          LOG.info(String.format("Ignoring same directory link %s to %s",
              jarPath.toString(), link.toString()));
          return true;
        }
      } catch (NotLinkException ex) {
        LOG.debug("Not a link", jar);
      } catch (IOException ex) {
        LOG.warn("Cannot read symbolic link on", jar);
      }
    }
    return false;
  }

  @VisibleForTesting
  boolean parseArguments(String[] args) throws IOException {
    Options opts = new Options();
    opts.addOption(Option.builder("h").build());
    opts.addOption(Option.builder("help").build());
    opts.addOption(Option.builder("input")
        .desc("Input class path. Defaults to the default classpath.")
        .hasArg().build());
    opts.addOption(Option.builder("whitelist")
        .desc(
            "Regex specifying the full path of jars to include in the" +
                " framework tarball. Default is a hardcoded set of jars" +
                " considered necessary to include")
        .hasArg().build());
    opts.addOption(Option.builder("blacklist")
        .desc(
            "Regex specifying the full path of jars to exclude in the" +
                " framework tarball. Default is a hardcoded set of jars" +
                " considered unnecessary to include")
        .hasArg().build());
    opts.addOption(Option.builder("fs")
        .desc(
            "Target file system to upload to." +
            " Example: hdfs://foo.com:8020")
        .hasArg().build());
    opts.addOption(Option.builder("target")
        .desc(
            "Target file to upload to with a reference name." +
                " Example: /usr/mr-framework.tar.gz#mr-framework")
        .hasArg().build());
    opts.addOption(Option.builder("initialReplication")
        .desc(
            "Desired initial replication count. Default 3.")
        .hasArg().build());
    opts.addOption(Option.builder("finalReplication")
        .desc(
            "Desired final replication count. Default 10.")
        .hasArg().build());
    opts.addOption(Option.builder("acceptableReplication")
        .desc(
            "Desired acceptable replication count. Default 9.")
        .hasArg().build());
    opts.addOption(Option.builder("timeout")
        .desc(
            "Desired timeout for the acceptable" +
                " replication in seconds. Default 10")
        .hasArg().build());
    opts.addOption(Option.builder("nosymlink")
        .desc("Ignore symlinks into the same directory")
        .build());
    GenericOptionsParser parser = new GenericOptionsParser(opts, args);
    if (parser.getCommandLine().hasOption("help") ||
        parser.getCommandLine().hasOption("h")) {
      printHelp(opts);
      return false;
    }
    input = parser.getCommandLine().getOptionValue(
        "input", System.getProperty("java.class.path"));
    whitelist = parser.getCommandLine().getOptionValue(
        "whitelist", DefaultJars.DEFAULT_MR_JARS);
    blacklist = parser.getCommandLine().getOptionValue(
        "blacklist", DefaultJars.DEFAULT_EXCLUDED_MR_JARS);
    initialReplication =
        Short.parseShort(parser.getCommandLine().getOptionValue(
            "initialReplication", "3"));
    finalReplication =
        Short.parseShort(parser.getCommandLine().getOptionValue(
            "finalReplication", "10"));
    acceptableReplication =
        Short.parseShort(
            parser.getCommandLine().getOptionValue(
                "acceptableReplication", "9"));
    timeout =
        Integer.parseInt(
            parser.getCommandLine().getOptionValue("timeout", "10"));
    if (parser.getCommandLine().hasOption("nosymlink")) {
      ignoreSymlink = true;
    }
    String fs = parser.getCommandLine()
        .getOptionValue("fs", null);
    String path = parser.getCommandLine().getOptionValue("target",
        "/usr/lib/mr-framework.tar.gz#mr-framework");
    boolean isFullPath =
        path.startsWith("hdfs://") ||
        path.startsWith("file://");

    if (fs == null) {
      fs = conf.get(FS_DEFAULT_NAME_KEY);
      if (fs == null && !isFullPath) {
        LOG.error("No filesystem specified in either fs or target.");
        printHelp(opts);
        return false;
      } else {
        LOG.info(String.format(
            "Target file system not specified. Using default %s", fs));
      }
    }
    if (path.isEmpty()) {
      LOG.error("Target directory not specified");
      printHelp(opts);
      return false;
    }
    StringBuilder absolutePath = new StringBuilder();
    if (!isFullPath) {
      absolutePath.append(fs);
      absolutePath.append(path.startsWith("/") ? "" : "/");
    }
    absolutePath.append(path);
    target = absolutePath.toString();

    if (parser.getRemainingArgs().length > 0) {
      LOG.warn("Unexpected parameters");
      printHelp(opts);
      return false;
    }
    return true;
  }

  /**
   * Tool entry point.
   * @param args arguments
   * @throws IOException thrown on configuration errors
   */
  public static void main(String[] args) throws IOException {
    FrameworkUploader uploader = new FrameworkUploader();
    if(uploader.parseArguments(args)) {
      uploader.run();
    }
  }
}
