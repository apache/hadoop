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
package org.apache.hadoop.hdfs.tools.offlineImageViewer;

import org.apache.hadoop.thirdparty.com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * The PBImageCorruptionDetector detects corruptions in the image.
 * It produces a file with the found issues similar to the Delimited
 * processor. The default delimiter is tab, as this is an unlikely value
 * to be included in an inode path. The delimiter value can be changed
 * via the constructor.
 *
 * It looks for the following kinds of corruptions:
 *  - an INode id is mentioned in the INodeDirectorySection, but not present
 *    in the INodeSection (corrupt INode case)
 *  - an INode has children, but at least one of them is corrupted
 *    (missing children case)
 * If multiple layers of directory structure are damaged then it is possible
 * that an INode is corrupted and also having corrupted children.
 *
 * Note that the OIV DetectCorruption processor check is not exhaustive,
 * and only catches the corruptions like above. This processor may be up to
 * extension in the future when new aspects of corruption are found.
 */
public class PBImageCorruptionDetector extends PBImageTextWriter {
  private static final Logger LOG =
      LoggerFactory.getLogger(PBImageCorruptionDetector.class);

  /**
   * Builder object for producing entries (lines) for
   * PBImageCorruptionDetector. The isSnapshot field is mandatory.
   */
  static class OutputEntryBuilder {
    private static final String MISSING = "Missing";

    private PBImageCorruptionDetector corrDetector;
    private PBImageCorruption corruption;
    private boolean isSnapshot;
    private String parentPath;
    private long parentId;
    private String name;
    private String nodeType;

    OutputEntryBuilder(PBImageCorruptionDetector corrDetector,
        boolean isSnapshot) {
      this.corrDetector = corrDetector;
      this.isSnapshot = isSnapshot;
      this.parentId = -1;
      this.parentPath = "";
      this.name = "";
      this.nodeType = "";
    }

    OutputEntryBuilder setCorruption(PBImageCorruption corr) {
      this.corruption = corr;
      return this;
    }

    OutputEntryBuilder setParentPath(String path) {
      this.parentPath = path;
      return this;
    }

    OutputEntryBuilder setParentId(long id) {
      this.parentId = id;
      return this;
    }

    OutputEntryBuilder setName(String n) {
      this.name = n;
      return this;
    }

    OutputEntryBuilder setNodeType(String nType) {
      this.nodeType = nType;
      return this;
    }

    public String build() {
      StringBuffer buffer = new StringBuffer();
      buffer.append(corruption.getType());
      corrDetector.append(buffer, corruption.getId());
      corrDetector.append(buffer, String.valueOf(isSnapshot));
      corrDetector.append(buffer, parentPath);
      if (parentId == -1) {
        corrDetector.append(buffer, MISSING);
      } else {
        corrDetector.append(buffer, parentId);
      }
      corrDetector.append(buffer, name);
      corrDetector.append(buffer, nodeType);
      corrDetector.append(buffer, corruption.getNumOfCorruptChildren());
      return buffer.toString();
    }
  }

  private static class CorruptionChecker {
    private static final String NODE_TYPE = "Node";
    private static final String REF_TYPE = "Ref";
    private static final String UNKNOWN_TYPE = "Unknown";

    /** Contains all existing INode IDs. */
    private Set<Long> nodeIds;
    /** Contains all existing INodeReference IDs. */
    private Set<Long> nodeRefIds;

    CorruptionChecker() {
      nodeIds = new HashSet<>();
    }

    /**
     * Collect a INode Id.
     */
    void saveNodeId(long id) {
      Preconditions.checkState(nodeIds != null && !nodeIds.contains(id));
      nodeIds.add(id);
    }

    /**
     * Returns whether the given INode id was saved previously.
     */
    boolean isNodeIdExist(long id) {
      return nodeIds.contains(id);
    }

    /**
     * Returns whether the given INodeReference id was saved previously.
     */
    boolean isNodeRefIdExist(long id) {
      return nodeRefIds.contains(id);
    }

    /**
     * Saves the INodeReference ids.
     */
    void saveNodeRefIds(List<Long> nodeRefIdList) {
      nodeRefIds = new HashSet<>(nodeRefIdList);
    }

    String getTypeOfId(long id) {
      if (isNodeIdExist(id)) {
        return NODE_TYPE;
      } else if (isNodeRefIdExist(id)) {
        return REF_TYPE;
      } else {
        return UNKNOWN_TYPE;
      }
    }
  }

  /** Delimiter string used while producing output. */
  private final CorruptionChecker corrChecker;
  /** Id to corruption mapping. */
  private final Map<Long, PBImageCorruption> corruptionsMap;

  PBImageCorruptionDetector(PrintStream out, String delimiter,
        String tempPath) throws IOException {
    super(out, delimiter, tempPath);
    corrChecker = new CorruptionChecker();
    corruptionsMap = new TreeMap<Long, PBImageCorruption>();
  }

  @Override
  public String getHeader() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("CorruptionType");
    append(buffer, "Id");
    append(buffer, "IsSnapshot");
    append(buffer, "ParentPath");
    append(buffer, "ParentId");
    append(buffer, "Name");
    append(buffer, "NodeType");
    append(buffer, "CorruptChildren");
    return buffer.toString();
  }

  @Override
  public String getEntry(String parentPath,
      FsImageProto.INodeSection.INode inode) {
    long id = inode.getId();
    if (corruptionsMap.containsKey(id)) {
      OutputEntryBuilder entryBuilder =
          new OutputEntryBuilder(this, false);
      long parentId = -1;
      try {
        parentId = getParentId(id);
      } catch (IOException ignore) {
      }
      entryBuilder.setCorruption(corruptionsMap.get(id))
          .setParentPath(parentPath)
          .setName(inode.getName().toStringUtf8())
          .setNodeType(corrChecker.getTypeOfId(id));
      if (parentId != -1) {
        entryBuilder.setParentId(parentId);
      }
      corruptionsMap.remove(id);
      return entryBuilder.build();
    } else {
      return "";
    }
  }

  @Override
  protected void checkNode(FsImageProto.INodeSection.INode p,
        AtomicInteger numDirs) throws IOException {
    super.checkNode(p, numDirs);
    corrChecker.saveNodeId(p.getId());
  }

  private void addCorruptedNode(long childId) {
    if (!corruptionsMap.containsKey(childId)) {
      PBImageCorruption c = new PBImageCorruption(childId, false, true, 0);
      corruptionsMap.put(childId, c);
    } else {
      PBImageCorruption c = corruptionsMap.get(childId);
      c.addCorruptNodeCorruption();
      corruptionsMap.put(childId, c);
    }
  }

  private void addCorruptedParent(long id, int numOfCorruption) {
    if (!corruptionsMap.containsKey(id)) {
      PBImageCorruption c = new PBImageCorruption(id, true, false,
          numOfCorruption);
      corruptionsMap.put(id, c);
    } else {
      PBImageCorruption c = corruptionsMap.get(id);
      c.addMissingChildCorruption();
      c.setNumberOfCorruption(numOfCorruption);
      corruptionsMap.put(id, c);
    }
  }

  /**
   * Scan the INodeDirectory section to construct the namespace.
   */
  @Override
  protected void buildNamespace(InputStream in, List<Long> refIdList)
      throws IOException {
    corrChecker.saveNodeRefIds(refIdList);
    LOG.debug("Saved INodeReference ids of size {}.", refIdList.size());
    int count = 0;
    while (true) {
      FsImageProto.INodeDirectorySection.DirEntry e =
          FsImageProto.INodeDirectorySection.DirEntry.parseDelimitedFrom(in);
      if (e == null) {
        break;
      }
      count++;
      if (LOG.isDebugEnabled() && count % 10000 == 0) {
        LOG.debug("Scanned {} directories.", count);
      }
      long parentId = e.getParent();
      if (!corrChecker.isNodeIdExist(parentId)) {
        LOG.debug("Corruption detected! Parent node is not contained " +
            "in the list of known ids!");
        addCorruptedNode(parentId);
      }
      int numOfCorruption = 0;
      for (int i = 0; i < e.getChildrenCount(); i++) {
        long childId = e.getChildren(i);
        putDirChildToMetadataMap(parentId, childId);
        if (!corrChecker.isNodeIdExist(childId)) {
          addCorruptedNode(childId);
          numOfCorruption++;
        }
      }
      if (numOfCorruption > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("{} corruption detected! Child nodes are missing.",
              numOfCorruption);
        }
        addCorruptedParent(parentId, numOfCorruption);
      }
      for (int i = e.getChildrenCount();
           i < e.getChildrenCount() + e.getRefChildrenCount(); i++) {
        int refId = e.getRefChildren(i - e.getChildrenCount());
        // In this case the refNode is referred directly (by its position),
        // so we couldn't make sure of the correctness
        putDirChildToMetadataMap(parentId, refIdList.get(refId));
      }
    }
    LOG.info("Scanned {} INode directories to build namespace.", count);
  }

  @Override
  public void afterOutput() throws IOException {
    if (!corruptionsMap.isEmpty()) {
      // Also write out corruptions when the path could be not be decided
      LOG.info("Outputting {} more corrupted nodes.", corruptionsMap.size());
      for (PBImageCorruption c : corruptionsMap.values()) {
        long id = c.getId();
        String name = "";
        long parentId = -1;
        try {
          name = getNodeName(id);
        } catch (IgnoreSnapshotException ignored) {
        }
        try {
          parentId = getParentId(id);
        } catch (IgnoreSnapshotException ignored) {
        }
        OutputEntryBuilder entryBuilder =
            new OutputEntryBuilder(this, true);
        entryBuilder.setCorruption(corruptionsMap.get(id))
            .setName(name)
            .setNodeType(corrChecker.getTypeOfId(id));
        if (parentId != -1) {
          entryBuilder.setParentId(parentId);
        }
        printIfNotEmpty(entryBuilder.build());
      }
    }
  }
}
