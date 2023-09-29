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

package org.apache.hadoop.fs.obs;

import com.obs.services.model.ListObjectsRequest;
import com.obs.services.model.ObjectListing;
import com.obs.services.model.ObjectMetadata;
import com.obs.services.model.ObsObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Queue;
import java.util.Stack;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * OBS depth first search listing implementation for posix bucket.
 */
class OBSFsDFSListing extends ObjectListing {
  /**
   * Class logger.
   */
  private static final Logger LOG = LoggerFactory.getLogger(
      OBSFsDFSListing.class);

  static void increaseLevelStats(final List<LevelStats> levelStatsList,
      final int level,
      final boolean isDir) {
    int currMaxLevel = levelStatsList.size() - 1;
    if (currMaxLevel < level) {
      for (int i = 0; i < level - currMaxLevel; i++) {
        levelStatsList.add(new LevelStats(currMaxLevel + 1 + i));
      }
    }

    if (isDir) {
      levelStatsList.get(level).increaseDirNum();
    } else {
      levelStatsList.get(level).increaseFileNum();
    }
  }

  static String fsDFSListNextBatch(final OBSFileSystem owner,
      final Stack<ListEntity> listStack,
      final Queue<ListEntity> resultQueue,
      final String marker,
      final int maxKeyNum,
      final List<ObsObject> objectSummaries,
      final List<LevelStats> levelStatsList) throws IOException {
    // 0. check if marker matches with the peek of result queue when marker
    // is given
    if (marker != null) {
      if (resultQueue.isEmpty()) {
        throw new IllegalArgumentException(
            "result queue is empty, but marker is not empty: "
                + marker);
      } else if (resultQueue.peek().getType()
          == ListEntityType.LIST_TAIL) {
        throw new RuntimeException(
            "cannot put list tail (" + resultQueue.peek()
                + ") into result queue");
      } else if (!marker.equals(
          resultQueue.peek().getType() == ListEntityType.COMMON_PREFIX
              ? resultQueue.peek().getCommonPrefix()
              : resultQueue.peek().getObjectSummary().getObjectKey())) {
        throw new IllegalArgumentException("marker (" + marker
            + ") does not match with result queue peek ("
            + resultQueue.peek() + ")");
      }
    }

    // 1. fetch some list results from local result queue
    int resultNum = fetchListResultLocally(owner.getBucket(), resultQueue,
        maxKeyNum, objectSummaries,
        levelStatsList);

    // 2. fetch more list results by doing one-level lists in parallel
    fetchListResultRemotely(owner, listStack, resultQueue, maxKeyNum,
        objectSummaries, levelStatsList, resultNum);

    // 3. check if list operation ends
    if (!listStack.empty() && resultQueue.isEmpty()) {
      throw new RuntimeException(
          "result queue is empty, but list stack is not empty: "
              + listStack);
    }

    String nextMarker = null;
    if (!resultQueue.isEmpty()) {
      if (resultQueue.peek().getType() == ListEntityType.LIST_TAIL) {
        throw new RuntimeException(
            "cannot put list tail (" + resultQueue.peek()
                + ") into result queue");
      } else {
        nextMarker =
            resultQueue.peek().getType() == ListEntityType.COMMON_PREFIX
                ? resultQueue
                .peek().getCommonPrefix()
                : resultQueue.peek().getObjectSummary().getObjectKey();
      }
    }
    return nextMarker;
  }

  static void fetchListResultRemotely(final OBSFileSystem owner,
      final Stack<ListEntity> listStack,
      final Queue<ListEntity> resultQueue, final int maxKeyNum,
      final List<ObsObject> objectSummaries,
      final List<LevelStats> levelStatsList,
      final int resultNum) throws IOException {
    int newResultNum = resultNum;
    while (!listStack.empty() && (newResultNum < maxKeyNum
        || resultQueue.isEmpty())) {
      List<ListObjectsRequest> oneLevelListRequests = new ArrayList<>();
      List<Future<ObjectListing>> oneLevelListFutures = new ArrayList<>();
      List<Integer> levels = new ArrayList<>();
      List<ObjectListing> oneLevelObjectListings = new ArrayList<>();
      // a. submit some one-level list tasks in parallel
      submitOneLevelListTasks(owner, listStack, maxKeyNum,
          oneLevelListRequests, oneLevelListFutures, levels);

      // b. wait these tasks to complete
      waitForOneLevelListTasksFinished(oneLevelListRequests,
          oneLevelListFutures, oneLevelObjectListings);

      // c. put subdir/file into result commonPrefixes and
      // objectSummaries;if the number of results reaches maxKeyNum,
      // cache it into resultQueue for next list batch  note: unlike
      // standard DFS, we put subdir directly into result list to avoid
      // caching it using more space
      newResultNum = handleOneLevelListTaskResult(resultQueue, maxKeyNum,
          objectSummaries, levelStatsList, newResultNum,
          oneLevelListRequests, levels, oneLevelObjectListings);

      // d. push subdirs and list continuing tail/end into list stack in
      // reversed order,so that we can pop them from the stack in order
      // later
      addNewListStackEntities(listStack, oneLevelListRequests, levels,
          oneLevelObjectListings);
    }
  }

  @SuppressWarnings("checkstyle:parameternumber")
  static int handleOneLevelListTaskResult(final Queue<ListEntity> resultQueue,
      final int maxKeyNum,
      final List<ObsObject> objectSummaries,
      final List<LevelStats> levelStatsList,
      final int resultNum,
      final List<ListObjectsRequest> oneLevelListRequests,
      final List<Integer> levels,
      final List<ObjectListing> oneLevelObjectListings) {
    int newResultNum = resultNum;
    for (int i = 0; i < oneLevelObjectListings.size(); i++) {
      LOG.debug(
          "one level listing with prefix=" + oneLevelListRequests.get(i)
              .getPrefix()
              + ", marker=" + (
              oneLevelListRequests.get(i).getMarker() != null
                  ? oneLevelListRequests.get(i)
                  .getMarker()
                  : ""));

      ObjectListing oneLevelObjectListing = oneLevelObjectListings.get(i);
      LOG.debug("# of CommonPrefixes/Objects: {}/{}",
          oneLevelObjectListing.getCommonPrefixes().size(),
          oneLevelObjectListing.getObjects().size());

      if (oneLevelObjectListing.getCommonPrefixes().isEmpty()
          && oneLevelObjectListing.getObjects().isEmpty()) {
        continue;
      }

      for (String commonPrefix
          : oneLevelObjectListing.getCommonPrefixes()) {
        if (commonPrefix.equals(
            oneLevelListRequests.get(i).getPrefix())) {
          // skip prefix itself
          continue;
        }

        LOG.debug("common prefix: " + commonPrefix);
        if (newResultNum < maxKeyNum) {
          addCommonPrefixIntoObjectList(
              oneLevelListRequests.get(i).getBucketName(),
              objectSummaries,
              commonPrefix);
          increaseLevelStats(levelStatsList, levels.get(i), true);
          newResultNum++;
        } else {
          resultQueue.add(
              new ListEntity(commonPrefix, levels.get(i)));
        }
      }

      for (ObsObject obj : oneLevelObjectListing.getObjects()) {
        if (obj.getObjectKey()
            .equals(oneLevelListRequests.get(i).getPrefix())) {
          // skip prefix itself
          continue;
        }

        LOG.debug("object: {}, size: {}", obj.getObjectKey(),
            obj.getMetadata().getContentLength());
        if (newResultNum < maxKeyNum) {
          objectSummaries.add(obj);
          increaseLevelStats(levelStatsList, levels.get(i),
              obj.getObjectKey().endsWith("/"));
          newResultNum++;
        } else {
          resultQueue.add(new ListEntity(obj, levels.get(i)));
        }
      }
    }
    return newResultNum;
  }

  static void waitForOneLevelListTasksFinished(
      final List<ListObjectsRequest> oneLevelListRequests,
      final List<Future<ObjectListing>> oneLevelListFutures,
      final List<ObjectListing> oneLevelObjectListings)
      throws IOException {
    for (int i = 0; i < oneLevelListFutures.size(); i++) {
      try {
        oneLevelObjectListings.add(oneLevelListFutures.get(i).get());
      } catch (InterruptedException e) {
        LOG.warn("Interrupted while listing using DFS, prefix="
            + oneLevelListRequests.get(i).getPrefix() + ", marker="
            + (oneLevelListRequests.get(i).getMarker() != null
            ? oneLevelListRequests.get(i).getMarker()
            : ""));
        throw new InterruptedIOException(
            "Interrupted while listing using DFS, prefix="
                + oneLevelListRequests.get(i).getPrefix() + ", marker="
                + (oneLevelListRequests.get(i).getMarker() != null
                ? oneLevelListRequests.get(i).getMarker()
                : ""));
      } catch (ExecutionException e) {
        LOG.error("Exception while listing using DFS, prefix="
                + oneLevelListRequests.get(i).getPrefix() + ", marker="
                + (oneLevelListRequests.get(i).getMarker() != null
                ? oneLevelListRequests.get(i).getMarker()
                : ""),
            e);
        for (Future<ObjectListing> future : oneLevelListFutures) {
          future.cancel(true);
        }

        throw OBSCommonUtils.extractException(
            "Listing using DFS with exception, marker="
                + (oneLevelListRequests.get(i).getMarker() != null
                ? oneLevelListRequests.get(i).getMarker()
                : ""),
            oneLevelListRequests.get(i).getPrefix(), e);
      }
    }
  }

  static void submitOneLevelListTasks(final OBSFileSystem owner,
      final Stack<ListEntity> listStack, final int maxKeyNum,
      final List<ListObjectsRequest> oneLevelListRequests,
      final List<Future<ObjectListing>> oneLevelListFutures,
      final List<Integer> levels) {
    for (int i = 0;
        i < owner.getListParallelFactor() && !listStack.empty(); i++) {
      ListEntity listEntity = listStack.pop();
      if (listEntity.getType() == ListEntityType.LIST_TAIL) {
        if (listEntity.getNextMarker() != null) {
          ListObjectsRequest oneLevelListRequest
              = new ListObjectsRequest();
          oneLevelListRequest.setBucketName(owner.getBucket());
          oneLevelListRequest.setPrefix(listEntity.getPrefix());
          oneLevelListRequest.setMarker(listEntity.getNextMarker());
          oneLevelListRequest.setMaxKeys(
              Math.min(maxKeyNum, owner.getMaxKeys()));
          oneLevelListRequest.setDelimiter("/");
          oneLevelListRequests.add(oneLevelListRequest);
          oneLevelListFutures.add(owner.getBoundedListThreadPool()
              .submit(() -> OBSCommonUtils.commonContinueListObjects(
                  owner, oneLevelListRequest)));
          levels.add(listEntity.getLevel());
        }

        // avoid adding list tasks in different levels later
        break;
      } else {
        String oneLevelListPrefix =
            listEntity.getType() == ListEntityType.COMMON_PREFIX
                ? listEntity.getCommonPrefix()
                : listEntity.getObjectSummary().getObjectKey();
        ListObjectsRequest oneLevelListRequest = OBSCommonUtils
            .createListObjectsRequest(owner, oneLevelListPrefix, "/",
                maxKeyNum);
        oneLevelListRequests.add(oneLevelListRequest);
        oneLevelListFutures.add(owner.getBoundedListThreadPool()
            .submit(() -> OBSCommonUtils.commonListObjects(owner,
                oneLevelListRequest)));
        levels.add(listEntity.getLevel() + 1);
      }
    }
  }

  static void addNewListStackEntities(final Stack<ListEntity> listStack,
      final List<ListObjectsRequest> oneLevelListRequests,
      final List<Integer> levels,
      final List<ObjectListing> oneLevelObjectListings) {
    for (int i = oneLevelObjectListings.size() - 1; i >= 0; i--) {
      ObjectListing oneLevelObjectListing = oneLevelObjectListings.get(i);

      if (oneLevelObjectListing.getCommonPrefixes().isEmpty()
          && oneLevelObjectListing.getObjects()
          .isEmpty()) {
        continue;
      }

      listStack.push(new ListEntity(oneLevelObjectListing.getPrefix(),
          oneLevelObjectListing.isTruncated()
              ? oneLevelObjectListing.getNextMarker()
              : null,
          levels.get(i)));

      ListIterator<String> commonPrefixListIterator
          = oneLevelObjectListing.getCommonPrefixes()
          .listIterator(oneLevelObjectListing.getCommonPrefixes().size());
      while (commonPrefixListIterator.hasPrevious()) {
        String commonPrefix = commonPrefixListIterator.previous();

        if (commonPrefix.equals(
            oneLevelListRequests.get(i).getPrefix())) {
          // skip prefix itself
          continue;
        }

        listStack.push(new ListEntity(commonPrefix, levels.get(i)));
      }

      ListIterator<ObsObject> objectSummaryListIterator
          = oneLevelObjectListing.getObjects()
          .listIterator(oneLevelObjectListing.getObjects().size());
      while (objectSummaryListIterator.hasPrevious()) {
        ObsObject objectSummary = objectSummaryListIterator.previous();

        if (objectSummary.getObjectKey()
            .equals(oneLevelListRequests.get(i).getPrefix())) {
          // skip prefix itself
          continue;
        }

        if (objectSummary.getObjectKey().endsWith("/")) {
          listStack.push(
              new ListEntity(objectSummary, levels.get(i)));
        }
      }
    }
  }

  static int fetchListResultLocally(final String bucketName,
      final Queue<ListEntity> resultQueue, final int maxKeyNum,
      final List<ObsObject> objectSummaries,
      final List<LevelStats> levelStatsList) {
    int resultNum = 0;
    while (!resultQueue.isEmpty() && resultNum < maxKeyNum) {
      ListEntity listEntity = resultQueue.poll();
      if (listEntity.getType() == ListEntityType.LIST_TAIL) {
        throw new RuntimeException("cannot put list tail (" + listEntity
            + ") into result queue");
      } else if (listEntity.getType() == ListEntityType.COMMON_PREFIX) {
        addCommonPrefixIntoObjectList(bucketName, objectSummaries,
            listEntity.getCommonPrefix());
        increaseLevelStats(levelStatsList, listEntity.getLevel(), true);
        resultNum++;
      } else {
        objectSummaries.add(listEntity.getObjectSummary());
        increaseLevelStats(levelStatsList, listEntity.getLevel(),
            listEntity.getObjectSummary().getObjectKey().endsWith("/"));
        resultNum++;
      }
    }
    return resultNum;
  }

  static void addCommonPrefixIntoObjectList(final String bucketName,
      final List<ObsObject> objectSummaries,
      final String commonPrefix) {
    ObsObject objectSummary = new ObsObject();
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(0L);
    objectSummary.setBucketName(bucketName);
    objectSummary.setObjectKey(commonPrefix);
    objectSummary.setMetadata(objectMetadata);
    objectSummaries.add(objectSummary);
  }

  static OBSFsDFSListing fsDFSListObjects(final OBSFileSystem owner,
      final ListObjectsRequest request) throws IOException {
    List<ObsObject> objectSummaries = new ArrayList<>();
    List<String> commonPrefixes = new ArrayList<>();
    String bucketName = owner.getBucket();
    String prefix = request.getPrefix();
    int maxKeyNum = request.getMaxKeys();
    if (request.getDelimiter() != null) {
      throw new IllegalArgumentException(
          "illegal delimiter: " + request.getDelimiter());
    }
    if (request.getMarker() != null) {
      throw new IllegalArgumentException(
          "illegal marker: " + request.getMarker());
    }

    Stack<ListEntity> listStack = new Stack<>();
    Queue<ListEntity> resultQueue = new LinkedList<>();
    List<LevelStats> levelStatsList = new ArrayList<>();

    listStack.push(new ListEntity(prefix, 0));
    increaseLevelStats(levelStatsList, 0, true);

    String nextMarker = fsDFSListNextBatch(owner, listStack, resultQueue,
        null, maxKeyNum, objectSummaries,
        levelStatsList);

    if (nextMarker == null) {
      StringBuilder levelStatsStringBuilder = new StringBuilder();
      levelStatsStringBuilder.append("bucketName=").append(bucketName)
          .append(", prefix=").append(prefix).append(": ");
      for (LevelStats levelStats : levelStatsList) {
        levelStatsStringBuilder.append("level=")
            .append(levelStats.getLevel())
            .append(", dirNum=")
            .append(levelStats.getDirNum())
            .append(", fileNum=")
            .append(levelStats.getFileNum())
            .append("; ");
      }
      LOG.debug("[list level statistics info] "
          + levelStatsStringBuilder.toString());
    }

    return new OBSFsDFSListing(request,
        objectSummaries,
        commonPrefixes,
        nextMarker,
        listStack,
        resultQueue,
        levelStatsList);
  }

  static OBSFsDFSListing fsDFSContinueListObjects(final OBSFileSystem owner,
      final OBSFsDFSListing obsFsDFSListing)
      throws IOException {
    List<ObsObject> objectSummaries = new ArrayList<>();
    List<String> commonPrefixes = new ArrayList<>();
    String bucketName = owner.getBucket();
    String prefix = obsFsDFSListing.getPrefix();
    String marker = obsFsDFSListing.getNextMarker();
    int maxKeyNum = obsFsDFSListing.getMaxKeys();
    if (obsFsDFSListing.getDelimiter() != null) {
      throw new IllegalArgumentException(
          "illegal delimiter: " + obsFsDFSListing.getDelimiter());
    }

    Stack<ListEntity> listStack = obsFsDFSListing.getListStack();
    Queue<ListEntity> resultQueue = obsFsDFSListing.getResultQueue();
    List<LevelStats> levelStatsList = obsFsDFSListing.getLevelStatsList();

    String nextMarker = fsDFSListNextBatch(owner, listStack, resultQueue,
        marker, maxKeyNum, objectSummaries,
        levelStatsList);

    if (nextMarker == null) {
      StringBuilder levelStatsStringBuilder = new StringBuilder();
      levelStatsStringBuilder.append("bucketName=").append(bucketName)
          .append(", prefix=").append(prefix).append(": ");
      for (LevelStats levelStats : levelStatsList) {
        levelStatsStringBuilder.append("level=")
            .append(levelStats.getLevel())
            .append(", dirNum=")
            .append(levelStats.getDirNum())
            .append(", fileNum=")
            .append(levelStats.getFileNum())
            .append("; ");
      }
      LOG.debug("[list level statistics info] "
          + levelStatsStringBuilder.toString());
    }

    return new OBSFsDFSListing(obsFsDFSListing,
        objectSummaries,
        commonPrefixes,
        nextMarker,
        listStack,
        resultQueue,
        levelStatsList);
  }

  /**
   * List entity type definition.
   */
  enum ListEntityType {
    /**
     * Common prefix.
     */
    COMMON_PREFIX,
    /**
     * Object summary.
     */
    OBJECT_SUMMARY,
    /**
     * List tail.
     */
    LIST_TAIL
  }

  /**
   * List entity for OBS depth first search listing.
   */
  static class ListEntity {
    /**
     * List entity type.
     */
    private ListEntityType type;

    /**
     * Entity level.
     */
    private final int level;

    /**
     * For COMMON_PREFIX.
     */
    private String commonPrefix = null;

    /**
     * For OBJECT_SUMMARY.
     */
    private ObsObject objectSummary = null;

    /**
     * For LIST_TAIL.
     */
    private String prefix = null;

    /**
     * Next marker.
     */
    private String nextMarker = null;

    ListEntity(final String comPrefix, final int entityLevel) {
      this.type = ListEntityType.COMMON_PREFIX;
      this.commonPrefix = comPrefix;
      this.level = entityLevel;
    }

    ListEntity(final ObsObject summary, final int entityLevel) {
      this.type = ListEntityType.OBJECT_SUMMARY;
      this.objectSummary = summary;
      this.level = entityLevel;
    }

    ListEntity(final String pf, final String nextMk,
        final int entityLevel) {
      this.type = ListEntityType.LIST_TAIL;
      this.prefix = pf;
      this.nextMarker = nextMk;
      this.level = entityLevel;
    }

    ListEntityType getType() {
      return type;
    }

    int getLevel() {
      return level;
    }

    String getCommonPrefix() {
      return commonPrefix;
    }

    ObsObject getObjectSummary() {
      return objectSummary;
    }

    public String getPrefix() {
      return prefix;
    }

    String getNextMarker() {
      return nextMarker;
    }

    @Override
    public String toString() {
      return "type: " + type
          + ", commonPrefix: " + (commonPrefix != null
          ? commonPrefix
          : "")
          + ", objectSummary: " + (objectSummary != null
          ? objectSummary
          : "")
          + ", prefix: " + (prefix != null ? prefix : "")
          + ", nextMarker: " + (nextMarker != null ? nextMarker : "");
    }
  }

  /**
   * Level statistics for OBS depth first search listing.
   */
  static class LevelStats {
    /**
     * Entity level.
     */
    private int level;

    /**
     * Directory num.
     */
    private long dirNum;

    /**
     * File num.
     */
    private long fileNum;

    LevelStats(final int entityLevel) {
      this.level = entityLevel;
      this.dirNum = 0;
      this.fileNum = 0;
    }

    void increaseDirNum() {
      dirNum++;
    }

    void increaseFileNum() {
      fileNum++;
    }

    int getLevel() {
      return level;
    }

    long getDirNum() {
      return dirNum;
    }

    long getFileNum() {
      return fileNum;
    }
  }

  /**
   * Stack of entity list..
   */
  private Stack<ListEntity> listStack;

  /**
   * Queue of entity list.
   */
  private Queue<ListEntity> resultQueue;

  /**
   * List of levelStats.
   */
  private List<LevelStats> levelStatsList;

  OBSFsDFSListing(final ListObjectsRequest request,
      final List<ObsObject> objectSummaries,
      final List<String> commonPrefixes,
      final String nextMarker,
      final Stack<ListEntity> listEntityStack,
      final Queue<ListEntity> listEntityQueue,
      final List<LevelStats> listLevelStats) {
    super(objectSummaries,
        commonPrefixes,
        request.getBucketName(),
        nextMarker != null,
        request.getPrefix(),
        null,
        request.getMaxKeys(),
        null,
        nextMarker,
        null);
    this.listStack = listEntityStack;
    this.resultQueue = listEntityQueue;
    this.levelStatsList = listLevelStats;
  }

  OBSFsDFSListing(final OBSFsDFSListing obsFsDFSListing,
      final List<ObsObject> objectSummaries,
      final List<String> commonPrefixes,
      final String nextMarker,
      final Stack<ListEntity> listEntityStack,
      final Queue<ListEntity> listEntityQueue,
      final List<LevelStats> listLevelStats) {
    super(objectSummaries,
        commonPrefixes,
        obsFsDFSListing.getBucketName(),
        nextMarker != null,
        obsFsDFSListing.getPrefix(),
        obsFsDFSListing.getNextMarker(),
        obsFsDFSListing.getMaxKeys(),
        null,
        nextMarker,
        null);
    this.listStack = listEntityStack;
    this.resultQueue = listEntityQueue;
    this.levelStatsList = listLevelStats;
  }

  Stack<ListEntity> getListStack() {
    return listStack;
  }

  Queue<ListEntity> getResultQueue() {
    return resultQueue;
  }

  List<LevelStats> getLevelStatsList() {
    return levelStatsList;
  }
}
