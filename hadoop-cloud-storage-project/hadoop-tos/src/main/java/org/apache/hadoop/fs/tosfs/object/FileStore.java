/*
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

package org.apache.hadoop.fs.tosfs.object;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.tosfs.conf.ConfKeys;
import org.apache.hadoop.fs.tosfs.conf.FileStoreKeys;
import org.apache.hadoop.fs.tosfs.object.exceptions.NotAppendableException;
import org.apache.hadoop.fs.tosfs.object.request.ListObjectsRequest;
import org.apache.hadoop.fs.tosfs.object.response.ListObjectsResponse;
import org.apache.hadoop.fs.tosfs.util.CommonUtils;
import org.apache.hadoop.fs.tosfs.util.Range;
import org.apache.hadoop.fs.tosfs.util.UUIDUtils;
import org.apache.hadoop.thirdparty.com.google.common.base.Strings;
import org.apache.hadoop.util.Lists;
import org.apache.hadoop.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileStore implements ObjectStorage {

  private static final Logger LOG = LoggerFactory.getLogger(FileStore.class);

  private static final String NAME = "filestore";
  private static final String STAGING_DIR = "__STAGING__";
  private static final String SLASH = "/";

  public static final String DEFAULT_BUCKET = "dummy-bucket";
  public static final String ENV_FILE_STORAGE_ROOT = "FILE_STORAGE_ROOT";

  private static final int MAX_DELETE_OBJECTS_COUNT = 1000;
  private static final int MIN_PART_SIZE = 5 * 1024 * 1024;
  private static final int MAX_PART_COUNT = 10000;

  private String bucket;
  private String root;
  private Configuration conf;
  private ChecksumInfo checksumInfo;

  @Override
  public String scheme() {
    return NAME;
  }

  @Override
  public BucketInfo bucket() {
    return new BucketInfo(bucket, false);
  }

  @Override
  public void initialize(Configuration config, String bucketName) {
    this.bucket = bucketName;
    this.conf = config;
    String endpoint = config.get(ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(NAME));
    if (endpoint == null || endpoint.isEmpty()) {
      endpoint = System.getenv(ENV_FILE_STORAGE_ROOT);
    }
    Preconditions.checkNotNull(endpoint, "%s cannot be null",
        ConfKeys.FS_OBJECT_STORAGE_ENDPOINT.key(NAME));

    if (endpoint.endsWith(SLASH)) {
      this.root = endpoint;
    } else {
      this.root = endpoint + SLASH;
    }
    LOG.debug("the root path is: {}", this.root);

    String algorithm = config.get(FileStoreKeys.FS_FILESTORE_CHECKSUM_ALGORITHM,
        FileStoreKeys.FS_FILESTORE_CHECKSUM_ALGORITHM_DEFAULT);
    ChecksumType checksumType = ChecksumType.valueOf(
        config.get(FileStoreKeys.FS_FILESTORE_CHECKSUM_TYPE,
            FileStoreKeys.FS_FILESTORE_CHECKSUM_TYPE_DEFAULT).toUpperCase());
    Preconditions.checkArgument(checksumType == ChecksumType.MD5,
        "Checksum type %s is not supported by FileStore.", checksumType.name());
    checksumInfo = new ChecksumInfo(algorithm, checksumType);

    Preconditions.checkState(new File(root).mkdirs(), "Failed to create root directory %s.", root);
  }

  @Override
  public Configuration conf() {
    return conf;
  }

  private static String encode(String key) {
    try {
      return URLEncoder.encode(key, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.warn("failed to encode key: {}", key);
      return key;
    }
  }

  private static String decode(String key) {
    try {
      return URLDecoder.decode(key, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      LOG.warn("failed to decode key: {}", key);
      return key;
    }
  }

  @Override
  public ObjectContent get(String key, long offset, long limit) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "Key should not be empty.");
    File file = path(encode(key)).toFile();
    if (!file.exists()) {
      throw new RuntimeException(String.format("File not found %s", file.getAbsolutePath()));
    }

    Range range = ObjectUtils.calculateRange(offset, limit, file.length());
    try (FileInputStream in = new FileInputStream(file)) {
      in.skip(range.off());
      byte[] bs = new byte[(int) range.len()];
      in.read(bs);

      byte[] fileChecksum = getFileChecksum(file.toPath());
      return new ObjectContent(fileChecksum, new ByteArrayInputStream(bs));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public byte[] put(String key, InputStreamProvider streamProvider, long contentLength) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "Key should not be empty.");
    File destFile = path(encode(key)).toFile();
    copyInputStreamToFile(streamProvider.newStream(), destFile, contentLength);

    return ObjectInfo.isDir(key) ? Constants.MAGIC_CHECKSUM : getFileChecksum(destFile.toPath());
  }

  @Override
  public byte[] append(String key, InputStreamProvider streamProvider, long contentLength) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "Key should not be empty.");
    File destFile = path(encode(key)).toFile();
    if (!destFile.exists()) {
      if (contentLength == 0) {
        throw new NotAppendableException(String.format(
            "%s is not appendable because append non-existed object with "
                + "zero byte is not supported.", key));
      }
      return put(key, streamProvider, contentLength);
    } else {
      appendInputStreamToFile(streamProvider.newStream(), destFile, contentLength);
      return ObjectInfo.isDir(key) ? Constants.MAGIC_CHECKSUM : getFileChecksum(destFile.toPath());
    }
  }

  private static File createTmpFile(File destFile) {
    String tmpFilename = ".tmp." + UUIDUtils.random();
    File file = new File(destFile.getParentFile(), tmpFilename);

    try {
      if (!file.exists() && !file.createNewFile()) {
        throw new RuntimeException("failed to create tmp file");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return file;
  }

  @Override
  public void delete(String key) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "Key should not be empty.");
    File file = path(encode(key)).toFile();
    if (file.exists()) {
      try {
        if (file.isDirectory()) {
          FileUtils.deleteDirectory(file);
        } else {
          Files.delete(file.toPath());
        }
      } catch (IOException ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  @Override
  public List<String> batchDelete(List<String> keys) {
    Preconditions.checkArgument(keys.size() <= MAX_DELETE_OBJECTS_COUNT,
        "The batch delete object count should <= %s", MAX_DELETE_OBJECTS_COUNT);
    List<String> failedKeys = Lists.newArrayList();
    for (String key : keys) {
      try {
        delete(key);
      } catch (Exception e) {
        LOG.error("Failed to delete key {}", key, e);
        failedKeys.add(key);
      }
    }
    return failedKeys;
  }

  @Override
  public void deleteAll(String prefix) {
    Iterable<ObjectInfo> objects = listAll(prefix, "");
    ObjectUtils.deleteAllObjects(this, objects, conf.getInt(
        ConfKeys.FS_BATCH_DELETE_SIZE.key(NAME),
        ConfKeys.FS_BATCH_DELETE_SIZE_DEFAULT));
  }

  @Override
  public ObjectInfo head(String key) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "Key should not be empty.");
    File file = path(encode(key)).toFile();
    if (file.exists()) {
      return toObjectInfo(file.toPath());
    } else {
      return null;
    }
  }

  @Override
  public Iterable<ListObjectsResponse> list(ListObjectsRequest request) {
    try (Stream<Path> stream = Files.walk(Paths.get(root))) {
      List<ObjectInfo> allObjects = list(stream, request.prefix(), request.startAfter())
          .collect(Collectors.toList());
      int maxKeys = request.maxKeys() < 0 ? allObjects.size() : request.maxKeys();
      return Collections.singletonList(
          splitObjects(request.prefix(), request.delimiter(), maxKeys, request.startAfter(),
              allObjects));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private ListObjectsResponse splitObjects(
      String prefix,
      String delimiter,
      int limit,
      String startAfter,
      List<ObjectInfo> objects) {
    int retSize = Math.min(limit, objects.size());

    if (Strings.isNullOrEmpty(delimiter)) {
      // the response only contains objects
      List<ObjectInfo> retObjs = objects.subList(0, retSize);
      return new ListObjectsResponse(retObjs, Collections.emptyList());
    } else {
      // the response only contains objects and common prefixes
      Set<String> commonPrefixes = new TreeSet<>();
      List<ObjectInfo> objectInfos = new ArrayList<>();

      for (ObjectInfo obj : objects) {
        String suffixKey = obj.key().substring(prefix.length());
        String[] tokens = suffixKey.split(delimiter, 2);
        if (tokens.length == 2) {
          String key = prefix + tokens[0] + delimiter;
          // the origin key is bigger than startAfter,
          // but after the new key after split might equal to startAfter, need to exclude.
          if (!key.equals(startAfter)) {
            commonPrefixes.add(key);

            // why don't break the loop before add new key to common prefixes list
            // is that the new key might be an existed common prefix, but we still want to continue
            // visited new object since the new object might also be an existed common prefix until
            // the total size is out of limit.
            if (commonPrefixes.size() + objectInfos.size() > retSize) {
              commonPrefixes.remove(key);
              break;
            }
          }
        } else {
          if (commonPrefixes.size() + objectInfos.size() >= retSize) {
            break;
          }

          objectInfos.add(obj);
        }
      }
      return new ListObjectsResponse(objectInfos, new ArrayList<>(commonPrefixes));
    }
  }

  @Override
  public MultipartUpload createMultipartUpload(String key) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "Key should not be empty.");

    String uploadId = UUIDUtils.random();
    Path uploadDir = uploadPath(key, uploadId);
    if (uploadDir.toFile().mkdirs()) {
      return new MultipartUpload(key, uploadId, MIN_PART_SIZE, MAX_PART_COUNT);
    } else {
      throw new RuntimeException("Failed to create MultipartUpload with key: " + key);
    }
  }

  private Path uploadPath(String key, String uploadId) {
    return Paths.get(root, STAGING_DIR, encode(key), uploadId);
  }

  @Override
  public Part uploadPart(
      String key, String uploadId, int partNum,
      InputStreamProvider streamProvider, long contentLength) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "Key should not be empty.");

    File uploadDir = uploadPath(key, uploadId).toFile();
    if (!uploadDir.exists()) {
      throw new RuntimeException("cannot locate the upload id: " + uploadId);
    }

    File partFile = new File(uploadDir, String.valueOf(partNum));
    copyInputStreamToFile(streamProvider.newStream(), partFile, contentLength);

    try {
      byte[] data = Files.readAllBytes(partFile.toPath());
      return new Part(partNum, data.length, DigestUtils.md5Hex(data));
    } catch (IOException e) {
      LOG.error("failed to locate the part file: {}", partFile.getAbsolutePath());
      throw new RuntimeException(e);
    }
  }

  private static void appendInputStreamToFile(InputStream in, File partFile, long contentLength) {
    try (FileOutputStream out = new FileOutputStream(partFile, true)) {
      long copiedBytes = IOUtils.copyLarge(in, out, 0, contentLength);

      if (copiedBytes < contentLength) {
        throw new IOException(String.format("Unexpect end of stream, expected to write length:%s,"
                + " actual written:%s", contentLength, copiedBytes));
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      CommonUtils.runQuietly(in::close);
    }
  }

  private static void copyInputStreamToFile(InputStream in, File partFile, long contentLength) {
    File tmpFile = createTmpFile(partFile);
    try (FileOutputStream out = new FileOutputStream(tmpFile)) {
      long copiedBytes = IOUtils.copyLarge(in, out, 0, contentLength);

      if (copiedBytes < contentLength) {
        throw new IOException(
            String.format("Unexpect end of stream, expected length:%s, actual:%s", contentLength,
                tmpFile.length()));
      }
    } catch (IOException e) {
      CommonUtils.runQuietly(() -> FileUtils.delete(tmpFile));
      throw new RuntimeException(e);
    } finally {
      CommonUtils.runQuietly(in::close);
    }

    if (!tmpFile.renameTo(partFile)) {
      throw new RuntimeException("failed to put file since rename fail.");
    }
  }

  @Override
  public byte[] completeUpload(String key, String uploadId, List<Part> uploadParts) {
    Preconditions.checkArgument(uploadParts != null && uploadParts.size() > 0,
        "upload parts cannot be null or empty.");
    File uploadDir = uploadPath(key, uploadId).toFile();
    if (!uploadDir.exists()) {
      throw new RuntimeException("cannot locate the upload id: " + uploadId);
    }

    List<Integer> partNums = listPartNums(uploadDir);
    if (partNums.size() != uploadParts.size()) {
      throw new RuntimeException(String.format("parts length mismatched: %d != %d",
          partNums.size(), uploadParts.size()));
    }

    Collections.sort(partNums);
    uploadParts.sort(Comparator.comparingInt(Part::num));

    Path keyPath = path(encode(key));
    File tmpFile = createTmpFile(keyPath.toFile());
    try (FileOutputStream outputStream = new FileOutputStream(tmpFile);
        FileChannel outputChannel = outputStream.getChannel()) {
      int offset = 0;
      for (int i = 0; i < partNums.size(); i++) {
        Part part = uploadParts.get(i);
        if (part.num() != partNums.get(i)) {
          throw new RuntimeException(
              String.format("part num mismatched: %d != %d", part.num(), partNums.get(i)));
        }

        File partFile = new File(uploadDir, String.valueOf(part.num()));
        checkPartFile(part, partFile);

        try (FileInputStream inputStream = new FileInputStream(partFile);
            FileChannel inputChannel = inputStream.getChannel()) {
          outputChannel.transferFrom(inputChannel, offset, partFile.length());
          offset += partFile.length();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (!tmpFile.renameTo(keyPath.toFile())) {
      throw new RuntimeException("rename file failed");
    } else {
      try {
        FileUtils.deleteDirectory(uploadDir);
      } catch (IOException e) {
        LOG.warn("failed to clean upload directory.");
      }
    }

    return getFileChecksum(keyPath);
  }

  private byte[] getFileChecksum(Path keyPath) {
    return getFileMD5(keyPath);
  }

  private static byte[] getFileMD5(Path keyPath) {
    try {
      return DigestUtils.md5(Files.readAllBytes(keyPath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static void checkPartFile(Part part, File partFile) throws IOException {
    if (part.size() != partFile.length()) {
      throw new RuntimeException(String.format("part size mismatched: %d != %d",
          part.size(), partFile.length()));
    }

    try (FileInputStream inputStream = new FileInputStream(partFile)) {
      String md5Hex = DigestUtils.md5Hex(inputStream);
      if (!Objects.equals(part.eTag(), md5Hex)) {
        throw new RuntimeException(String.format("part etag mismatched: %s != %s",
            part.eTag(), md5Hex));
      }
    }
  }

  private List<Integer> listPartNums(File uploadDir) {
    try (Stream<Path> stream = Files.list(uploadDir.toPath())) {
      return stream
          .map(f -> Integer.valueOf(f.toFile().getName()))
          .collect(Collectors.toList());
    } catch (IOException e) {
      LOG.error("failed to list part files.");
      throw new RuntimeException(e);
    }
  }

  @Override
  public void abortMultipartUpload(String key, String uploadId) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(key), "Key should not be empty.");
    Path uploadDir = uploadPath(key, uploadId);
    if (uploadDir.toFile().exists()) {
      try {
        FileUtils.deleteDirectory(uploadDir.toFile());
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public Iterable<MultipartUpload> listUploads(String prefix) {
    Path stagingDir = Paths.get(root, STAGING_DIR);
    if (!Files.exists(stagingDir)) {
      return Collections.emptyList();
    }
    try (Stream<Path> encodedKeyStream = Files.list(stagingDir)) {
      return encodedKeyStream
          .filter(key -> Objects.equals(prefix, "") ||
              key.toFile().getName().startsWith(encode(prefix)))
          .flatMap(key -> {
            try {
              return Files.list(key)
                  .map(id -> new MultipartUpload(decode(key.toFile().getName()),
                      id.toFile().getName(), MIN_PART_SIZE, MAX_PART_COUNT));
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          })
          .sorted()
          .collect(Collectors.toList());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Part uploadPartCopy(
      String srcKey, String dstKey, String uploadId, int partNum, long copySourceRangeStart,
      long copySourceRangeEnd) {
    File uploadDir = uploadPath(dstKey, uploadId).toFile();
    if (!uploadDir.exists()) {
      throw new RuntimeException(String.format("Upload directory %s already exits", uploadDir));
    }
    File partFile = new File(uploadDir, String.valueOf(partNum));
    int fileSize = (int) (copySourceRangeEnd - copySourceRangeStart + 1);
    try (InputStream is = get(srcKey, copySourceRangeStart, fileSize).stream();
        FileOutputStream fos = new FileOutputStream(partFile)) {
      byte[] data = new byte[fileSize];
      IOUtils.readFully(is, data);
      fos.write(data);
      return new Part(partNum, fileSize, DigestUtils.md5Hex(data));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void copy(String srcKey, String dstKey) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(srcKey), "Src key should not be empty.");
    File file = path(encode(srcKey)).toFile();
    if (!file.exists()) {
      throw new RuntimeException(String.format("File not found %s", file.getAbsolutePath()));
    }

    put(dstKey, () -> get(srcKey).stream(), file.length());
  }

  @Override
  public void rename(String srcKey, String dstKey) {
    Preconditions.checkArgument(!Objects.equals(srcKey, dstKey),
        "Cannot rename to the same object");
    Preconditions.checkNotNull(head(srcKey), "Source key %s doesn't exist", srcKey);

    File srcFile = path(encode(srcKey)).toFile();
    File dstFile = path(encode(dstKey)).toFile();
    boolean ret = srcFile.renameTo(dstFile);
    if (!ret) {
      throw new RuntimeException(String.format("Failed to rename %s to %s", srcKey, dstKey));
    }
  }

  @Override
  public ObjectInfo objectStatus(String key) {
    ObjectInfo obj = head(key);
    if (obj == null && !ObjectInfo.isDir(key)) {
      key = key + '/';
      obj = head(key);
    }

    if (obj == null) {
      Iterable<ObjectInfo> objs = list(key, null, 1);
      if (objs.iterator().hasNext()) {
        obj = new ObjectInfo(key, 0, new Date(0), Constants.MAGIC_CHECKSUM);
      }
    }

    return obj;
  }

  @Override
  public ChecksumInfo checksumInfo() {
    return checksumInfo;
  }

  private Stream<ObjectInfo> list(Stream<Path> stream, String prefix, String startAfter) {
    return stream
        .filter(p -> {
          String absolutePath = p.toFile().getAbsolutePath();
          return !Objects.equals(key(absolutePath), "") &&
              decode(key(absolutePath)).startsWith(prefix)
              && !absolutePath.contains(STAGING_DIR)
              && filter(decode(key(absolutePath)), startAfter);
        })
        .map(this::toObjectInfo)
        .sorted(Comparator.comparing(ObjectInfo::key));
  }

  private boolean filter(String key, String startAfter) {
    if (Strings.isNullOrEmpty(startAfter)) {
      return true;
    } else {
      return key.compareTo(startAfter) > 0;
    }
  }

  private ObjectInfo toObjectInfo(Path path) {
    File file = path.toFile();
    String key = decode(key(file.getAbsolutePath()));
    return new ObjectInfo(key, file.length(), new Date(file.lastModified()),
        getFileChecksum(path));
  }

  private Path path(String key) {
    return Paths.get(root, key);
  }

  private String key(String path) {
    if (path.length() < root.length()) {
      // root = path + "/"
      return "";
    }
    return path.substring(root.length());
  }

  @Override
  public void close() throws IOException {
  }
}
