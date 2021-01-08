/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.InvalidParameterException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;
import com.amazonaws.services.dynamodbv2.xspec.ExpressionSpecBuilder;
import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;
import org.apache.hadoop.fs.s3a.S3AFileSystem;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.util.StopWatch;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.itemToPathMetadata;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.pathToKey;
import static org.apache.hadoop.fs.s3a.s3guard.PathMetadataDynamoDBTranslation.pathToParentKey;

/**
 * Main class for the FSCK factored out from S3GuardTool
 * The implementation uses fixed DynamoDBMetadataStore as the backing store
 * for metadata.
 *
 * Functions:
 * <ul>
 *   <li>Checking metadata consistency between S3 and metadatastore</li>
 *   <li>Checking the internal metadata consistency</li>
 * </ul>
 */
public class S3GuardFsck {
  private static final Logger LOG = LoggerFactory.getLogger(S3GuardFsck.class);
  public static final String ROOT_PATH_STRING = "/";

  private final S3AFileSystem rawFS;
  private final DynamoDBMetadataStore metadataStore;

  private static final long MOD_TIME_RANGE = 2000L;

  /**
   * Creates an S3GuardFsck.
   * @param fs the filesystem to compare to
   * @param ms metadatastore the metadatastore to compare with (dynamo)
   */
  public S3GuardFsck(S3AFileSystem fs, MetadataStore ms)
      throws InvalidParameterException {
    this.rawFS = fs;

    if (ms == null) {
      throw new InvalidParameterException("S3A Bucket " + fs.getBucket()
          + " should be guarded by a "
          + DynamoDBMetadataStore.class.getCanonicalName());
    }
    this.metadataStore = (DynamoDBMetadataStore) ms;

    Preconditions.checkArgument(!rawFS.hasMetadataStore(),
        "Raw fs should not have a metadatastore.");
  }

  /**
   * Compares S3 to MS.
   * Iterative breadth first walk on the S3 structure from a given root.
   * Creates a list of pairs (metadata in S3 and in the MetadataStore) where
   * the consistency or any rule is violated.
   * Uses {@link S3GuardFsckViolationHandler} to handle violations.
   * The violations are listed in Enums: {@link Violation}
   *
   * @param p the root path to start the traversal
   * @return a list of {@link ComparePair}
   * @throws IOException
   */
  public List<ComparePair> compareS3ToMs(Path p) throws IOException {
    StopWatch stopwatch = new StopWatch();
    stopwatch.start();
    int scannedItems = 0;

    final Path rootPath = rawFS.qualify(p);
    S3AFileStatus root = (S3AFileStatus) rawFS.getFileStatus(rootPath);
    final List<ComparePair> comparePairs = new ArrayList<>();
    final Queue<S3AFileStatus> queue = new ArrayDeque<>();
    queue.add(root);

    while (!queue.isEmpty()) {
      final S3AFileStatus currentDir = queue.poll();


      final Path currentDirPath = currentDir.getPath();
      try {
        List<FileStatus> s3DirListing = Arrays.asList(
            rawFS.listStatus(currentDirPath));

        // Check authoritative directory flag.
        compareAuthoritativeDirectoryFlag(comparePairs, currentDirPath,
            s3DirListing);
        // Add all descendant directory to the queue
        s3DirListing.stream().filter(pm -> pm.isDirectory())
            .map(S3AFileStatus.class::cast)
            .forEach(pm -> queue.add(pm));

        // Check file and directory metadata for consistency.
        final List<S3AFileStatus> children = s3DirListing.stream()
            .filter(status -> !status.isDirectory())
            .map(S3AFileStatus.class::cast).collect(toList());
        final List<ComparePair> compareResult =
            compareS3DirContentToMs(currentDir, children);
        comparePairs.addAll(compareResult);

        // Increase the scanned file size.
        // One for the directory, one for the children.
        scannedItems++;
        scannedItems += children.size();
      } catch (FileNotFoundException e) {
        LOG.error("The path has been deleted since it was queued: "
            + currentDirPath, e);
      }

    }
    stopwatch.stop();

    // Create a handler and handle each violated pairs
    S3GuardFsckViolationHandler handler =
        new S3GuardFsckViolationHandler(rawFS, metadataStore);
    for (ComparePair comparePair : comparePairs) {
      handler.logError(comparePair);
    }

    LOG.info("Total scan time: {}s", stopwatch.now(TimeUnit.SECONDS));
    LOG.info("Scanned entries: {}", scannedItems);

    return comparePairs;
  }

  /**
   * Compare the directory contents if the listing is authoritative.
   *
   * @param comparePairs the list of compare pairs to add to
   *                     if it contains a violation
   * @param currentDirPath the current directory path
   * @param s3DirListing the s3 directory listing to compare with
   * @throws IOException
   */
  private void compareAuthoritativeDirectoryFlag(List<ComparePair> comparePairs,
      Path currentDirPath, List<FileStatus> s3DirListing) throws IOException {
    final DirListingMetadata msDirListing =
        metadataStore.listChildren(currentDirPath);
    if (msDirListing != null && msDirListing.isAuthoritative()) {
      ComparePair cP = new ComparePair(s3DirListing, msDirListing);

      if (s3DirListing.size() != msDirListing.numEntries()) {
        cP.violations.add(Violation.AUTHORITATIVE_DIRECTORY_CONTENT_MISMATCH);
      } else {
        final Set<Path> msPaths = msDirListing.getListing().stream()
                .map(pm -> pm.getFileStatus().getPath()).collect(toSet());
        final Set<Path> s3Paths = s3DirListing.stream()
                .map(pm -> pm.getPath()).collect(toSet());
        if (!s3Paths.equals(msPaths)) {
          cP.violations.add(Violation.AUTHORITATIVE_DIRECTORY_CONTENT_MISMATCH);
        }
      }

      if (cP.containsViolation()) {
        comparePairs.add(cP);
      }
    }
  }

  /**
   * Compares S3 directory content to the metadata store.
   *
   * @param s3CurrentDir file status of the current directory
   * @param children the contents of the directory
   * @return the compare pairs with violations of consistency
   * @throws IOException
   */
  protected List<ComparePair> compareS3DirContentToMs(
      S3AFileStatus s3CurrentDir,
      List<S3AFileStatus> children) throws IOException {
    final Path path = s3CurrentDir.getPath();
    final PathMetadata pathMetadata = metadataStore.get(path);
    List<ComparePair> violationComparePairs = new ArrayList<>();

    final ComparePair rootComparePair =
        compareFileStatusToPathMetadata(s3CurrentDir, pathMetadata);
    if (rootComparePair.containsViolation()) {
      violationComparePairs.add(rootComparePair);
    }

    children.forEach(s3ChildMeta -> {
      try {
        final PathMetadata msChildMeta =
            metadataStore.get(s3ChildMeta.getPath());
        final ComparePair comparePair =
            compareFileStatusToPathMetadata(s3ChildMeta, msChildMeta);
        if (comparePair.containsViolation()) {
          violationComparePairs.add(comparePair);
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
      }
    });

    return violationComparePairs;
  }

  /**
   * Compares a {@link S3AFileStatus} from S3 to a {@link PathMetadata}
   * from the metadata store. Finds violated invariants and consistency
   * issues.
   *
   * @param s3FileStatus the file status from S3
   * @param msPathMetadata the path metadata from metadatastore
   * @return {@link ComparePair} with the found issues
   * @throws IOException
   */
  protected ComparePair compareFileStatusToPathMetadata(
      S3AFileStatus s3FileStatus,
      PathMetadata msPathMetadata) throws IOException {
    final Path path = s3FileStatus.getPath();

    if (msPathMetadata != null) {
      LOG.info("Path: {} - Length S3: {}, MS: {} " +
              "- Etag S3: {}, MS: {} ",
          path,
          s3FileStatus.getLen(), msPathMetadata.getFileStatus().getLen(),
          s3FileStatus.getETag(), msPathMetadata.getFileStatus().getETag());
    } else {
      LOG.info("Path: {} - Length S3: {} - Etag S3: {}, no record in MS.",
              path, s3FileStatus.getLen(), s3FileStatus.getETag());
    }

    ComparePair comparePair = new ComparePair(s3FileStatus, msPathMetadata);

    if (!path.equals(path(ROOT_PATH_STRING))) {
      final Path parentPath = path.getParent();
      final PathMetadata parentPm = metadataStore.get(parentPath);

      if (parentPm == null) {
        comparePair.violations.add(Violation.NO_PARENT_ENTRY);
      } else {
        if (!parentPm.getFileStatus().isDirectory()) {
          comparePair.violations.add(Violation.PARENT_IS_A_FILE);
        }
        if (parentPm.isDeleted()) {
          comparePair.violations.add(Violation.PARENT_TOMBSTONED);
        }
      }
    } else {
      LOG.debug("Entry is in the root directory, so there's no parent");
    }

    // If the msPathMetadata is null, we RETURN because
    // there is no metadata compare with
    if (msPathMetadata == null) {
      comparePair.violations.add(Violation.NO_METADATA_ENTRY);
      return comparePair;
    }

    final S3AFileStatus msFileStatus = msPathMetadata.getFileStatus();
    if (s3FileStatus.isDirectory() && !msFileStatus.isDirectory()) {
      comparePair.violations.add(Violation.DIR_IN_S3_FILE_IN_MS);
    }
    if (!s3FileStatus.isDirectory() && msFileStatus.isDirectory()) {
      comparePair.violations.add(Violation.FILE_IN_S3_DIR_IN_MS);
    }

    if(msPathMetadata.isDeleted()) {
      comparePair.violations.add(Violation.TOMBSTONED_IN_MS_NOT_DELETED_IN_S3);
    }

    /**
     * Attribute check
     */
    if (s3FileStatus.getLen() != msFileStatus.getLen()) {
      comparePair.violations.add(Violation.LENGTH_MISMATCH);
    }

    // ModTime should be in the accuracy range defined.
    long modTimeDiff = Math.abs(
        s3FileStatus.getModificationTime() - msFileStatus.getModificationTime()
    );
    if (modTimeDiff > MOD_TIME_RANGE) {
      comparePair.violations.add(Violation.MOD_TIME_MISMATCH);
    }

    if(msPathMetadata.getFileStatus().getVersionId() == null
        || s3FileStatus.getVersionId() == null) {
      LOG.debug("Missing versionIDs skipped. A HEAD request is "
          + "required for each object to get the versionID.");
    } else if(!s3FileStatus.getVersionId().equals(msFileStatus.getVersionId())) {
      comparePair.violations.add(Violation.VERSIONID_MISMATCH);
    }

    // check etag only for files, and not directories
    if (!s3FileStatus.isDirectory()) {
      if (msPathMetadata.getFileStatus().getETag() == null) {
        comparePair.violations.add(Violation.NO_ETAG);
      } else if (s3FileStatus.getETag() != null &&
          !s3FileStatus.getETag().equals(msFileStatus.getETag())) {
        comparePair.violations.add(Violation.ETAG_MISMATCH);
      }
    }

    return comparePair;
  }

  private Path path(String s) {
    return rawFS.makeQualified(new Path(s));
  }

  /**
   * Fix violations found during check.
   *
   * Currently only supports handling the following violation:
   * - Violation.ORPHAN_DDB_ENTRY
   *
   * @param violations to be handled
   * @throws IOException throws the error if there's any during handling
   */
  public void fixViolations(List<ComparePair> violations) throws IOException {
    S3GuardFsckViolationHandler handler =
        new S3GuardFsckViolationHandler(rawFS, metadataStore);

    for (ComparePair v : violations) {
      if (v.getViolations().contains(Violation.ORPHAN_DDB_ENTRY)) {
        try {
          handler.doFix(v);
        } catch (IOException e) {
          LOG.error("Error during handling the violation: ", e);
          throw e;
        }
      }
    }
  }

  /**
   * A compare pair with the pair of metadata and the list of violations.
   */
  public static class ComparePair {
    private final S3AFileStatus s3FileStatus;
    private final PathMetadata msPathMetadata;

    private final List<FileStatus> s3DirListing;
    private final DirListingMetadata msDirListing;

    private final Path path;

    private final Set<Violation> violations = new HashSet<>();

    ComparePair(S3AFileStatus status, PathMetadata pm) {
      this.s3FileStatus = status;
      this.msPathMetadata = pm;
      this.s3DirListing = null;
      this.msDirListing = null;
      if (status != null) {
        this.path = status.getPath();
      } else {
        this.path = pm.getFileStatus().getPath();
      }
    }

    ComparePair(List<FileStatus> s3DirListing, DirListingMetadata msDirListing) {
      this.s3DirListing = s3DirListing;
      this.msDirListing = msDirListing;
      this.s3FileStatus = null;
      this.msPathMetadata = null;
      this.path = msDirListing.getPath();
    }

    public S3AFileStatus getS3FileStatus() {
      return s3FileStatus;
    }

    public PathMetadata getMsPathMetadata() {
      return msPathMetadata;
    }

    public Set<Violation> getViolations() {
      return violations;
    }

    public boolean containsViolation() {
      return !violations.isEmpty();
    }

    public DirListingMetadata getMsDirListing() {
      return msDirListing;
    }

    public List<FileStatus> getS3DirListing() {
      return s3DirListing;
    }

    public Path getPath() {
      return path;
    }

    @Override public String toString() {
      return "ComparePair{" + "s3FileStatus=" + s3FileStatus
          + ", msPathMetadata=" + msPathMetadata + ", s3DirListing=" +
          s3DirListing + ", msDirListing=" + msDirListing + ", path="
          + path + ", violations=" + violations + '}';
    }
  }

  /**
   * Check the DynamoDB metadatastore internally for consistency.
   * <pre>
   * Tasks to do here:
   *  - find orphan entries (entries without a parent).
   *  - find if a file's parent is not a directory (so the parent is a file).
   *  - find entries where the parent is a tombstone.
   *  - warn: no lastUpdated field.
   * </pre>
   */
  public List<ComparePair> checkDdbInternalConsistency(Path basePath)
      throws IOException {
    Preconditions.checkArgument(basePath.isAbsolute(), "path must be absolute");

    List<ComparePair> comparePairs = new ArrayList<>();
    String rootStr = basePath.toString();
    LOG.info("Root for internal consistency check: {}", rootStr);
    StopWatch stopwatch = new StopWatch();
    stopwatch.start();

    final Table table = metadataStore.getTable();
    final String username = metadataStore.getUsername();
    DDBTree ddbTree = new DDBTree();

    /*
     * I. Root node construction
     * - If the root node is the real bucket root, a node is constructed instead of
     *   doing a query to the ddb because the bucket root is not stored.
     * - If the root node is not a real bucket root then the entry is queried from
     *   the ddb and constructed from the result.
     */

    DDBPathMetadata baseMeta;

    if (!basePath.isRoot()) {
      PrimaryKey rootKey = pathToKey(basePath);
      final GetItemSpec spec = new GetItemSpec()
          .withPrimaryKey(rootKey)
          .withConsistentRead(true);
      final Item baseItem = table.getItem(spec);
      baseMeta = itemToPathMetadata(baseItem, username);

      if (baseMeta == null) {
        throw new FileNotFoundException(
            "Base element metadata is null. " +
                "This means the base path element is missing, or wrong path was " +
                "passed as base path to the internal ddb consistency checker.");
      }
    } else {
      baseMeta = new DDBPathMetadata(
          new S3AFileStatus(Tristate.UNKNOWN, basePath, username)
      );
    }

    DDBTreeNode root = new DDBTreeNode(baseMeta);
    ddbTree.addNode(root);
    ddbTree.setRoot(root);

    /*
     * II. Build and check the descendant tree:
     * 1. query all nodes where the prefix is the given root, and put it in the tree
     * 2. Check connectivity: check if each parent is in the hashmap
     *    - This is done in O(n): we only need to find the parent based on the
     *      path with a hashmap lookup.
     *    - Do a test if the graph is connected - if the parent is not in the
     *      hashmap, we found an orphan entry.
     *
     * 3. Do test the elements for errors:
     *    - File is a parent of a file.
     *    - Entries where the parent is tombstoned but the entries are not.
     *    - Warn on no lastUpdated field.
     *
     */
    ExpressionSpecBuilder builder = new ExpressionSpecBuilder();
    builder.withCondition(
        ExpressionSpecBuilder.S("parent")
            .beginsWith(pathToParentKey(basePath))
    );
    final IteratorSupport<Item, ScanOutcome> resultIterator = table.scan(
        builder.buildForScan()).iterator();
    resultIterator.forEachRemaining(item -> {
      final DDBPathMetadata pmd = itemToPathMetadata(item, username);
      DDBTreeNode ddbTreeNode = new DDBTreeNode(pmd);
      ddbTree.addNode(ddbTreeNode);
    });

    LOG.debug("Root: {}", ddbTree.getRoot());

    for (Map.Entry<Path, DDBTreeNode> entry : ddbTree.getContentMap().entrySet()) {
      final DDBTreeNode node = entry.getValue();
      final ComparePair pair = new ComparePair(null, node.val);
      // let's skip the root node when checking.
      if (node.getVal().getFileStatus().getPath().isRoot()) {
        continue;
      }

      if(node.getVal().getLastUpdated() == 0) {
        pair.violations.add(Violation.NO_LASTUPDATED_FIELD);
      }

      // skip further checking the basenode which is not the actual bucket root.
      if (node.equals(ddbTree.getRoot())) {
        continue;
      }

      final Path parent = node.getFileStatus().getPath().getParent();
      final DDBTreeNode parentNode = ddbTree.getContentMap().get(parent);
      if (parentNode == null) {
        pair.violations.add(Violation.ORPHAN_DDB_ENTRY);
      } else {
        if (!node.isTombstoned() && !parentNode.isDirectory()) {
          pair.violations.add(Violation.PARENT_IS_A_FILE);
        }
        if(!node.isTombstoned() && parentNode.isTombstoned()) {
          pair.violations.add(Violation.PARENT_TOMBSTONED);
        }
      }

      if (!pair.violations.isEmpty()) {
        comparePairs.add(pair);
      }

      node.setParent(parentNode);
    }

    // Create a handler and handle each violated pairs
    S3GuardFsckViolationHandler handler =
        new S3GuardFsckViolationHandler(rawFS, metadataStore);
    for (ComparePair comparePair : comparePairs) {
      handler.logError(comparePair);
    }

    stopwatch.stop();
    LOG.info("Total scan time: {}s", stopwatch.now(TimeUnit.SECONDS));
    LOG.info("Scanned entries: {}", ddbTree.contentMap.size());

    return comparePairs;
  }

  /**
   * DDBTree is the tree that represents the structure of items in the DynamoDB.
   */
  public static class DDBTree {
    private final Map<Path, DDBTreeNode> contentMap = new HashMap<>();
    private DDBTreeNode root;

    public DDBTree() {
    }

    public Map<Path, DDBTreeNode> getContentMap() {
      return contentMap;
    }

    public DDBTreeNode getRoot() {
      return root;
    }

    public void setRoot(DDBTreeNode root) {
      this.root = root;
    }

    public void addNode(DDBTreeNode pm) {
      contentMap.put(pm.getVal().getFileStatus().getPath(), pm);
    }

    @Override
    public String toString() {
      return "DDBTree{" +
          "contentMap=" + contentMap +
          ", root=" + root +
          '}';
    }
  }

  /**
   * Tree node for DDBTree.
   */
  private static final class DDBTreeNode {
    private final DDBPathMetadata val;
    private DDBTreeNode parent;
    private final List<DDBPathMetadata> children;

    private DDBTreeNode(DDBPathMetadata pm) {
      this.val = pm;
      this.parent = null;
      this.children = new ArrayList<>();
    }

    public DDBPathMetadata getVal() {
      return val;
    }

    public DDBTreeNode getParent() {
      return parent;
    }

    public void setParent(DDBTreeNode parent) {
      this.parent = parent;
    }

    public List<DDBPathMetadata> getChildren() {
      return children;
    }

    public boolean isDirectory() {
      return val.getFileStatus().isDirectory();
    }

    public S3AFileStatus getFileStatus() {
      return val.getFileStatus();
    }

    public boolean isTombstoned() {
      return val.isDeleted();
    }

    @Override
    public String toString() {
      return "DDBTreeNode{" +
          "val=" + val +
          ", parent=" + parent +
          ", children=" + children +
          '}';
    }
  }

  /**
   * Violation with severity and the handler.
   * Defines the severity of the violation between 0-2
   * where 0 is the most severe and 2 is the least severe.
   */
  public enum Violation {
    /**
     * No entry in metadatastore.
     */
    NO_METADATA_ENTRY(1,
        S3GuardFsckViolationHandler.NoMetadataEntry.class),
    /**
     * A file or directory entry does not have a parent entry - excluding
     * files and directories in the root.
     */
    NO_PARENT_ENTRY(0,
        S3GuardFsckViolationHandler.NoParentEntry.class),
    /**
     * An entry’s parent is a file.
     */
    PARENT_IS_A_FILE(0,
        S3GuardFsckViolationHandler.ParentIsAFile.class),
    /**
     * A file exists under a path for which there is a
     * tombstone entry in the MS.
     */
    PARENT_TOMBSTONED(0,
        S3GuardFsckViolationHandler.ParentTombstoned.class),
    /**
     * A directory in S3 is a file entry in the MS.
     */
    DIR_IN_S3_FILE_IN_MS(0,
        S3GuardFsckViolationHandler.DirInS3FileInMs.class),
    /**
     * A file in S3 is a directory in the MS.
     */
    FILE_IN_S3_DIR_IN_MS(0,
        S3GuardFsckViolationHandler.FileInS3DirInMs.class),
    AUTHORITATIVE_DIRECTORY_CONTENT_MISMATCH(1,
        S3GuardFsckViolationHandler.AuthDirContentMismatch.class),
    /**
     * An entry in the MS is tombstoned, but the object is not deleted on S3.
     */
    TOMBSTONED_IN_MS_NOT_DELETED_IN_S3(0,
        S3GuardFsckViolationHandler.TombstonedInMsNotDeletedInS3.class),
    /**
     * Attribute mismatch.
     */
    LENGTH_MISMATCH(0,
        S3GuardFsckViolationHandler.LengthMismatch.class),
    MOD_TIME_MISMATCH(2,
        S3GuardFsckViolationHandler.ModTimeMismatch.class),
    /**
     * If there's a versionID the mismatch is severe.
     */
    VERSIONID_MISMATCH(0,
        S3GuardFsckViolationHandler.VersionIdMismatch.class),
    /**
     * If there's an etag the mismatch is severe.
     */
    ETAG_MISMATCH(0,
        S3GuardFsckViolationHandler.EtagMismatch.class),
    /**
     * Don't worry too much if we don't have an etag.
     */
    NO_ETAG(2,
        S3GuardFsckViolationHandler.NoEtag.class),
    /**
     * The entry does not have a parent in ddb.
     */
    ORPHAN_DDB_ENTRY(0, S3GuardFsckViolationHandler.OrphanDDBEntry.class),
    /**
     * The entry's lastUpdated field is empty.
     */
    NO_LASTUPDATED_FIELD(2,
        S3GuardFsckViolationHandler.NoLastUpdatedField.class);

    private final int severity;
    private final Class<? extends S3GuardFsckViolationHandler.ViolationHandler> handler;

    Violation(int s,
        Class<? extends S3GuardFsckViolationHandler.ViolationHandler> h) {
      this.severity = s;
      this.handler = h;
    }

    public int getSeverity() {
      return severity;
    }

    public Class<? extends S3GuardFsckViolationHandler.ViolationHandler> getHandler() {
      return handler;
    }
  }
}
