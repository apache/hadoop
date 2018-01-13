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

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SystemErasureCodingPolicies;
import org.apache.hadoop.io.IOUtils;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

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

  @VisibleForTesting
  String input = null;
  @VisibleForTesting
  String whitelist = null;
  @VisibleForTesting
  String blacklist = null;
  @VisibleForTesting
  String target = null;
  @VisibleForTesting
  short replication = 10;
  private boolean ignoreSymlink = false;

  @VisibleForTesting
  Set<String> filteredInputFiles = new HashSet<>();
  @VisibleForTesting
  List<Pattern> whitelistedFiles = new LinkedList<>();
  @VisibleForTesting
  List<Pattern> blacklistedFiles = new LinkedList<>();

  private OutputStream targetStream = null;
  private String alias = null;

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
    } catch (UploaderException|IOException e) {
      LOG.error("Error in execution " + e.getMessage());
      e.printStackTrace();
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
      validateTargetPath();
      int lastIndex = target.indexOf('#');
      Path targetPath =
          new Path(
              target.substring(
                  0, lastIndex == -1 ? target.length() : lastIndex));
      alias = lastIndex != -1 ?
          target.substring(lastIndex + 1) :
          targetPath.getName();
      LOG.info("Target " + targetPath);
      FileSystem fileSystem = targetPath.getFileSystem(new Configuration());

      targetStream = null;
      if (fileSystem instanceof DistributedFileSystem) {
        LOG.info("Set replication to " +
            replication + " for path: " + targetPath);
        LOG.info("Disabling Erasure Coding for path: " + targetPath);
        DistributedFileSystem dfs = (DistributedFileSystem)fileSystem;
        DistributedFileSystem.HdfsDataOutputStreamBuilder builder =
            dfs.createFile(targetPath)
            .overwrite(true)
            .ecPolicyName(
                SystemErasureCodingPolicies.getReplicationPolicy().getName());
        if (replication > 0) {
          builder.replication(replication);
        }
        targetStream = builder.build();
      } else {
        LOG.warn("Cannot set replication to " +
            replication + " for path: " + targetPath +
            " on a non-distributed fileystem " +
            fileSystem.getClass().getName());
      }
      if (targetStream == null) {
        targetStream = fileSystem.create(targetPath, true);
      }

      if (targetPath.getName().endsWith("gz") ||
          targetPath.getName().endsWith("tgz")) {
        LOG.info("Creating GZip");
        targetStream = new GZIPOutputStream(targetStream);
      }
    }
  }

  @VisibleForTesting
  void buildPackage() throws IOException, UploaderException {
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
        if (normalizedLinkPath != null && jarParent.equals(
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

  private void validateTargetPath() throws UploaderException {
    if (!target.startsWith("hdfs:/") &&
        !target.startsWith("file:/")) {
      throw new UploaderException("Target path is not hdfs or local " + target);
    }
  }

  @VisibleForTesting
  boolean parseArguments(String[] args) throws IOException {
    Options opts = new Options();
    opts.addOption(OptionBuilder.create("h"));
    opts.addOption(OptionBuilder.create("help"));
    opts.addOption(OptionBuilder
        .withDescription("Input class path")
        .hasArg().create("input"));
    opts.addOption(OptionBuilder
        .withDescription(
            "Regex specifying the full path of jars to include in the" +
                " framework tarball. Default is a hardcoded set of jars" +
                " considered necessary to include")
        .hasArg().create("whitelist"));
    opts.addOption(OptionBuilder
        .withDescription(
            "Regex specifying the full path of jars to exclude in the" +
                " framework tarball. Default is a hardcoded set of jars" +
                " considered unnecessary to include")
        .hasArg().create("blacklist"));
    opts.addOption(OptionBuilder
        .withDescription(
            "Target file system to upload to." +
            " Example: hdfs://foo.com:8020")
        .hasArg().create("fs"));
    opts.addOption(OptionBuilder
        .withDescription(
            "Target file to upload to with a reference name." +
                " Example: /usr/mr-framework.tar.gz#mr-framework")
        .hasArg().create("target"));
    opts.addOption(OptionBuilder
        .withDescription(
            "Desired replication count")
        .hasArg().create("replication"));
    opts.addOption(OptionBuilder
        .withDescription("Ignore symlinks into the same directory")
        .create("nosymlink"));
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
    replication = Short.parseShort(parser.getCommandLine().getOptionValue(
        "replication", "10"));
    if (parser.getCommandLine().hasOption("nosymlink")) {
      ignoreSymlink = true;
    }
    String fs = parser.getCommandLine()
        .getOptionValue("fs", null);
    if (fs == null) {
      LOG.error("Target file system not specified");
      printHelp(opts);
      return false;
    }
    String path = parser.getCommandLine().getOptionValue("target",
        "mr-framework.tar.gz#mr-framework");
    if (path == null) {
      LOG.error("Target directory not specified");
      printHelp(opts);
      return false;
    }
    StringBuilder absolutePath = new StringBuilder(fs);
    absolutePath = absolutePath.append(path.startsWith("/") ? "" : "/");
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
