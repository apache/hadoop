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

package org.apache.hadoop.runc.tools;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options.Rename;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.hadoop.runc.docker.DockerClient;
import org.apache.hadoop.runc.docker.DockerContext;
import org.apache.hadoop.runc.docker.DockerCoordinates;
import org.apache.hadoop.runc.docker.DockerException;
import org.apache.hadoop.runc.docker.model.BlobV2;
import org.apache.hadoop.runc.docker.model.ManifestListV2;
import org.apache.hadoop.runc.docker.model.ManifestRefV2;
import org.apache.hadoop.runc.docker.model.ManifestV2;
import org.apache.hadoop.runc.squashfs.SquashFsConverter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

public class ImportDockerImage extends Configured implements Tool {

  private static final String PUBLIC_DOCKER_REPO =
      "registry.hub.docker.com";

  private static final Logger LOG
      = LoggerFactory.getLogger(ImportDockerImage.class);

  public static final String IMPORT_PREFIX =
      YarnConfiguration.RUNC_CONTAINER_RUNTIME_PREFIX + "import.";

  public static final String DEFAULT_DOCKER_REGISTRY_KEY =
      IMPORT_PREFIX + "default-docker-registry";

  public static final String MK_RUNC_IMPORT_TYPE = "runc.import.type";
  public static final String MK_RUNC_IMPORT_SOURCE = "runc.import.source";
  public static final String MK_RUNC_IMPORT_TIME = "runc.import.time";
  public static final String MK_RUNC_MANIFEST = "runc.manifest";

  public static final String IT_DOCKER = "docker";

  public static final String DEFAULT_NS = "library";
  public static final String DEFAULT_TAG = "latest";

  private static final Pattern VALID_NS_PATTERN =
      Pattern.compile("^[A-Za-z0-9]+$");

  private static final Pattern VALID_NAME_PATTERN =
      Pattern.compile("^[~^+-\\._A-Za-z0-9]+$");

  private Configuration conf;
  private String defaultRegistry;
  private FileSystem fs;
  private FileContext fc;
  private Path repoPath;
  private Path lockPath;
  private Path metaPath;
  private Path configPath;
  private Path layerPath;
  private Path manifestPath;
  private File tmpDir;

  private String[] imageParts(String coordinates) {
    String namespace;
    String nameAndTag;
    String name;
    String tag;

    String[] parts = coordinates.split("/", -1);
    if (parts.length == 2) {
      namespace = parts[0];
      nameAndTag = parts[1];
    } else if (parts.length == 1) {
      namespace = DEFAULT_NS;
      nameAndTag = parts[0];
    } else {
      throw new IllegalArgumentException(
          "Invalid image coordinates: " + coordinates);
    }
    if (!VALID_NS_PATTERN.matcher(namespace).matches()) {
      throw new IllegalArgumentException(
          "Invalid image namespace: " + namespace);
    }

    String[] tagParts = nameAndTag.split(":", -1);
    if (tagParts.length == 2) {
      name = tagParts[0];
      tag = tagParts[1];
    } else if (tagParts.length == 1) {
      name = tagParts[0];
      tag = DEFAULT_TAG;
    } else {
      throw new IllegalArgumentException(
          "Invalid image name: " + nameAndTag);
    }

    if (!VALID_NAME_PATTERN.matcher(name).matches()) {
      throw new IllegalArgumentException("Invalid image name: " + name);
    }

    if (!VALID_NAME_PATTERN.matcher(tag).matches()) {
      throw new IllegalArgumentException("Invalid image tag: " + tag);
    }

    return new String[] {namespace, name, tag};
  }

  private void importDockerImage(String source, String destCoordinates)
      throws IOException, URISyntaxException, DockerException {

    String[] imageCoordinates = imageParts(destCoordinates);
    DockerCoordinates coord = new DockerCoordinates(defaultRegistry, source);
    LOG.debug("Using Docker coordinates {}", coord);
    Instant importTime = Instant.now();

    try (DockerClient client = new DockerClient()) {
      LOG.info("Fetching image '{}' from Docker repository at {}",
          coord.getImage(), coord.getBaseUrl());

      DockerContext context = client.createContext(coord.getBaseUrl());
      ManifestListV2 manifests = client.listManifests(
          context, coord.getImageName(), coord.getImageRef());
      ManifestRefV2 mref =
          client.getManifestChooser().chooseManifest(manifests);
      if (mref == null) {
        throw new DockerException("No matching manifest found");
      }

      byte[] manifestData = client.readManifest(
          context, coord.getImageName(), mref.getDigest());

      // write manifest
      String manifestHash = mref.getDigest().replaceAll("^sha256:", "");
      File manifestDir = new File(tmpDir, "manifest");
      if (!manifestDir.mkdirs()) {
        throw new IOException("Unable to create manifest directory");
      }
      File manifestFile = new File(manifestDir, manifestHash);
      try (FileOutputStream fos = new FileOutputStream(manifestFile)) {
        fos.write(manifestData);
      }

      ManifestV2 manifest = client.parseManifest(manifestData);

      String configDigest = manifest.getConfig().getDigest();

      byte[] config = client.readConfig(
          context, coord.getImageName(), configDigest);

      // write config
      String configHash = configDigest.replaceAll("^sha256:", "");
      File configDir = new File(tmpDir, "config");
      if (!configDir.mkdirs()) {
        throw new IOException("Unable to create config directory");
      }
      File configFile = new File(configDir, configHash);
      try (FileOutputStream fos = new FileOutputStream(configFile)) {
        fos.write(config);
      }

      // create layer dir
      File layerDir = new File(tmpDir, "layer");
      if (!layerDir.mkdirs()) {
        throw new IOException("Unable to create layer directory");
      }

      // download layers
      List<String> layersDownloaded = downloadLayers(
          client, context, coord, manifest, layerDir);

      // convert layers
      convertLayers(layersDownloaded, layerDir);

      // upload layers
      uploadLayers(layersDownloaded, layerDir);

      // upload config if needed
      uploadConfig(configFile, configHash, configDigest);

      // upload manifest if needed
      uploadManifest(manifestFile, mref, manifestHash, configDigest);

      // create/update metadata properties file
      uploadMetadata(source, mref, imageCoordinates, importTime);
    }
  }

  private List<String> downloadLayers(DockerClient client,
      DockerContext context, DockerCoordinates coord, ManifestV2 manifest,
      File layerDir) throws IOException, URISyntaxException, DockerException {
    List<String> layersDownloaded = new ArrayList<>();

    byte[] buf = new byte[32768];
    int count = manifest.getLayers().size();
    int current = 0;
    for (BlobV2 blob : manifest.getLayers()) {
      current++;
      String digest = blob.getDigest();
      String hash = digest.replaceAll("^sha256:", "");
      String hashDir = hash.substring(0, 2);

      // check for sqsh and tar.gz files
      Path tgzPath = new Path(layerPath, hashDir + "/" + hash + ".tar.gz");
      Path sqshPath = new Path(layerPath, hashDir + "/" + hash + ".sqsh");
      if (fs.exists(tgzPath) && fs.exists(sqshPath)) {
        LOG.info("Skipping up-to-date layer {} ({} of {})", digest, current,
            count);
        continue;
      }

      layersDownloaded.add(digest);

      LOG.info("Downloading layer {} ({} of {})", digest, current, count);
      try (InputStream is = client
          .download(context, coord.getImageName(), digest)) {
        File outputFile = new File(layerDir, hash + ".tar.gz");
        try (FileOutputStream os = new FileOutputStream(outputFile)) {
          int c;
          while ((c = is.read(buf, 0, buf.length)) >= 0) {
            if (c > 0) {
              os.write(buf, 0, c);
            }
          }
        }
      }

    }
    return layersDownloaded;
  }

  private void convertLayers(List<String> layersDownloaded, File layerDir)
      throws IOException {
    int count = layersDownloaded.size();
    int current = 0;
    for (String digest : layersDownloaded) {
      current++;
      LOG.info("Converting layer {} ({} of {})", digest, current, count);
      String hash = digest.replaceAll("^sha256:", "");

      File inputFile = new File(layerDir, hash + ".tar.gz");
      File outputFile = new File(layerDir, hash + ".sqsh");
      SquashFsConverter.convertToSquashFs(inputFile, outputFile);
    }
  }

  private void uploadLayers(List<String> layersDownloaded, File layerDir)
      throws IOException {
    int count = layersDownloaded.size();
    int current = 0;
    for (String digest : layersDownloaded) {
      current++;
      LOG.info("Uploading layer {} ({} of {})", digest, current, count);
      String hash = digest.replaceAll("^sha256:", "");

      File tgzFile = new File(layerDir, hash + ".tar.gz");
      File sqshFile = new File(layerDir, hash + ".sqsh");

      Path layerHashPath = new Path(layerPath, hash.substring(0, 2));

      Path tmpTgz = new Path(layerHashPath, "._TMP." + hash + ".tar.gz");
      Path tmpSqsh = new Path(layerHashPath, "._TMP." + hash + ".sqsh");

      Path tgz = new Path(layerHashPath, hash + ".tar.gz");
      Path sqsh = new Path(layerHashPath, hash + ".sqsh");

      uploadFile(tgzFile, tgz, tmpTgz);
      uploadFile(sqshFile, sqsh, tmpSqsh);
    }
  }

  private void uploadConfig(File configFile, String configHash,
      String configDigest) throws IOException {
    Path configHashPath = new Path(configPath, configHash.substring(0, 2));
    Path remoteConfigFile = new Path(configHashPath, configHash);
    if (fs.exists(remoteConfigFile)) {
      LOG.info("Skipping up-to-date config {}", configDigest);
    } else {
      LOG.info("Uploading config {}", configDigest);
      Path remoteTmp = new Path(configHashPath, "._TMP." + configHash);
      uploadFile(configFile, remoteConfigFile, remoteTmp);
    }
  }

  private void uploadManifest(File manifestFile, ManifestRefV2 mref,
      String manifestHash, String configDigest) throws IOException {
    Path manifestHashPath = new Path(
        manifestPath, manifestHash.substring(0, 2));
    Path remoteManifestFile = new Path(manifestHashPath, manifestHash);
    if (fs.exists(remoteManifestFile)) {
      LOG.info("Skipping up-to-date manifest {}", configDigest);
    } else {
      LOG.info("Uploading manifest {}", mref.getDigest());
      Path remoteTmp = new Path(manifestHashPath, "._TMP." + manifestHash);
      uploadFile(manifestFile, remoteManifestFile, remoteTmp);
    }
  }

  private void uploadMetadata(String source, ManifestRefV2 mref,
      String[] imageCoordinates, Instant importTime) throws IOException {
    File metaFile = new File(tmpDir, "meta.properties");
    File metaFileUpdated = new File(tmpDir, "meta.properties.new");
    Path nsPath = new Path(metaPath, imageCoordinates[0]);
    Path metadataPath = new Path(
        nsPath,
        imageCoordinates[1] + "@" + imageCoordinates[2] + ".properties");
    Path metadataPathTmp = new Path(
        nsPath, "._TMP." + imageCoordinates[1] + "@" +
        imageCoordinates[2] + ".properties");

    Properties metadata = new Properties();
    if (fs.exists(metadataPath)) {
      downloadFile(metadataPath, metaFile);
      try (FileInputStream fis = new FileInputStream(metaFile)) {
        metadata.load(fis);
      }
    }

    metadata.setProperty(MK_RUNC_IMPORT_TYPE, IT_DOCKER);
    metadata.setProperty(MK_RUNC_IMPORT_SOURCE, source);
    metadata.setProperty(MK_RUNC_MANIFEST,  mref.getDigest());
    metadata.setProperty(MK_RUNC_IMPORT_TIME, importTime.toString());

    try (FileOutputStream fos = new FileOutputStream(metaFileUpdated)) {
      metadata.store(fos, null);
    }

    LOG.info("Writing metadata properties");
    uploadFile(metaFileUpdated, metadataPath, metadataPathTmp);
  }

  private void downloadFile(Path remoteFile, File localFile)
      throws IOException {

    try (FSDataInputStream in = fs.open(remoteFile)) {
      try (FileOutputStream out = new FileOutputStream(localFile)) {
        IOUtils.copyBytes(in, out, 65536);
      }
    }
  }

  private void uploadFile(File localFile, Path remoteFile, Path remoteTmp)
      throws IOException {
    boolean success = false;
    Path tmpParent = remoteTmp.getParent();
    if (!fs.mkdirs(tmpParent)) {
      throw new IOException("Unable to make directory " + tmpParent);
    }
    Path remoteParent = remoteFile.getParent();
    if (!fs.mkdirs(remoteParent)) {
      throw new IOException("Unable to make directory " + remoteParent);
    }

    try (InputStream in = new FileInputStream(localFile)) {
      try (FSDataOutputStream out = fs.create(remoteTmp, (short) 10)) {
        IOUtils.copyBytes(in, out, 65536);
      }
      fc.rename(remoteTmp, remoteFile, Rename.OVERWRITE);
      success = true;
    } finally {
      if (!success) {
        if (!fs.delete(remoteTmp, false)) {
          throw new IOException("Unable to delete " + remoteTmp);
        }
      }
    }
  }

  private FSDataOutputStream createLockFile(int attempts, int sleepTimeMs)
      throws IOException {
    try {
      if (!fs.mkdirs(repoPath)) {
        throw new IOException("Unable to create directory " + repoPath);
      }
      FSDataOutputStream out = createLockFileWithRetries(
          FsPermission.getFileDefault(), attempts, sleepTimeMs);
      fs.deleteOnExit(lockPath);
      out.writeBytes(InetAddress.getLocalHost().toString());
      out.flush();
      out.hflush();
      return out;
    } catch (RemoteException e) {
      if (e.getClassName().contains("AlreadyBeingCreatedException")) {
        return null;
      } else {
        throw e;
      }
    }
  }

  private void unlock(FSDataOutputStream lockStream, int attempts,
      int sleepTimeMs) {
    int attempt = 1;
    do {
      try {
        IOUtils.closeStream(lockStream);
        if (!fs.delete(lockPath, false)) {
          throw new IOException("Unable to delete " + lockPath);
        }
        return;
      } catch (IOException ioe) {
        LOG.info("Failed to delete " + lockPath + ", try="
            + attempt + " of " + attempts);
        LOG.debug("Failed to delete " + lockPath, ioe);
        try {
          Thread.sleep(sleepTimeMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          LOG.warn("Interrupted while deleting lock file" + lockPath);
          return;
        }
      }
    } while (attempt < attempts);
  }

  private FSDataOutputStream createLockFileWithRetries(
      FsPermission defaultPerms, int attempts, int sleepTimeMs)
      throws IOException {
    IOException exception = null;
    int attempt = 1;
    do {
      try {
        return fs.create(
            lockPath,
            defaultPerms,
            false,
            fs.getConf().getInt(
                CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY,
                CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT),
            fs.getDefaultReplication(lockPath),
            fs.getDefaultBlockSize(lockPath),
            null);
      } catch (IOException ioe) {
        LOG.info("Failed to create lock file " + lockPath
            + ", try=" + attempt + " of " + attempts);
        LOG.debug("Failed to create lock file " + lockPath,
            ioe);
        try {
          exception = ioe;
          attempt++;
          if (attempt < attempts) {
            Thread.sleep(sleepTimeMs);
          }
        } catch (InterruptedException ie) {
          throw (InterruptedIOException) new InterruptedIOException(
              "Can't create lock file " + lockPath)
              .initCause(ie);
        }
      }
    } while (attempt < attempts);
    throw exception;
  }

  public void cleanup() {
    if (tmpDir != null) {
      deleteRecursive(tmpDir);
    }
  }

  private void deleteRecursive(File file) {
    if (file.isDirectory()) {
      File[] children = file.listFiles();
      if (children != null) {
        for (File sub : children) {
          deleteRecursive(sub);
        }
      }
    }
    if (!file.delete()) {
      LOG.trace("Unable to delete " + file);
    }
  }

  @Override
  public int run(String[] argv) throws Exception {
    conf = new YarnConfiguration(getConf());

    Options options = options();
    CommandLineParser parser = new PosixParser();
    CommandLine cmd;
    try {
      cmd = parser.parse(options, argv);
    } catch (ParseException e) {
      System.out.println(
          "Error parsing command-line options: " + e.getMessage());
      printUsage();
      return -1;
    }

    if (cmd.hasOption("h")) {
      printUsage();
      return -1;
    }

    for (Option o : cmd.getOptions()) {
      switch (o.getOpt()) {
      case "r":
        conf.set(DEFAULT_DOCKER_REGISTRY_KEY, o.getValue());
        break;
      default:
        throw new UnsupportedOperationException(
            "Unknown option: " + o.getOpt());
      }
    }

    String[] rem = cmd.getArgs();
    if (rem.length != 2) {
      printUsage();
      return -1;
    }

    String source = rem[0];
    String dest = rem[1];

    String repoDir = conf.get(
        YarnConfiguration.NM_RUNC_IMAGE_TOPLEVEL_DIR,
        YarnConfiguration.DEFAULT_NM_RUNC_IMAGE_TOPLEVEL_DIR);

    defaultRegistry =
        conf.get(DEFAULT_DOCKER_REGISTRY_KEY, PUBLIC_DOCKER_REPO);

    Runtime.getRuntime().addShutdownHook(new Thread(this::cleanup));

    repoPath = new Path(repoDir);
    fs = repoPath.getFileSystem(conf);
    fc = FileContext.getFileContext(conf);
    lockPath = new Path(repoPath, ".import.lock");
    metaPath = new Path(repoPath, "meta");
    manifestPath = new Path(repoPath, "manifest");
    configPath = new Path(repoPath, "config");
    layerPath = new Path(repoPath, "layer");
    tmpDir = Files.createTempDirectory("runc-import-").toFile();

    LOG.debug("Using default docker registry: {}", defaultRegistry);
    LOG.debug("Using top-level runc repository: {}", repoPath);
    LOG.debug("Using lock file: {}", lockPath);
    LOG.debug("Using temporary dir: {}", tmpDir);

    FSDataOutputStream lockStream = createLockFile(10, 30000);
    try {
      if (!fs.mkdirs(manifestPath)) {
        throw new IOException("Unable to create " + manifestPath);
      }
      if (!fs.mkdirs(configPath)) {
        throw new IOException("Unable to create " + configPath);
      }
      if (!fs.mkdirs(layerPath)) {
        throw new IOException("Unable to create " + layerPath);
      }
      importDockerImage(source, dest);
    } finally {
      unlock(lockStream, 10, 30000);
    }
    return 0;
  }

  protected void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(
        "import-docker-image [OPTIONS] <docker-image> <runc-image-name>",
        new Options());
    formatter.setSyntaxPrefix("");
    formatter.printHelp("Options", options());
    ToolRunner.printGenericCommandUsage(System.out);
  }

  static Options options() {
    Options options = new Options();
    options.addOption("h", "help", false, "Print usage");
    options.addOption("r", "repository", true, "Default Docker repository");
    return options;
  }

  public static void main(String[] argv) throws Exception {
    int ret = ToolRunner.run(new ImportDockerImage(), argv);
    System.exit(ret);
  }

}
