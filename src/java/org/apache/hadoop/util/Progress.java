/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.util;

import java.util.ArrayList;

/** Utility to assist with generation of progress reports.  Applications build
 * a hierarchy of {@link Progress} instances, each modelling a phase of
 * execution.  The root is constructed with {@link #Progress()}.  Nodes for
 * sub-phases are created by calling {@link #addPhase()}.
 */
public class Progress {
  private String status = "";
  private float progress;
  private int currentPhase;
  private ArrayList phases = new ArrayList();
  private Progress parent;
  private float progressPerPhase;

  /** Creates a new root node. */
  public Progress() {}

  /** Adds a named node to the tree. */
  public Progress addPhase(String status) {
    Progress phase = addPhase();
    phase.setStatus(status);
    return phase;
  }

  /** Adds a node to the tree. */
  public Progress addPhase() {
    Progress phase = new Progress();
    phases.add(phase);
    phase.parent = this;
    progressPerPhase = 1.0f / (float)phases.size();
    return phase;
  }

  /** Called during execution to move to the next phase at this level in the
   * tree. */
  public void startNextPhase() {
    currentPhase++;
  }

  /** Returns the current sub-node executing. */
  public Progress phase() {
    return (Progress)phases.get(currentPhase);
  }

  /** Completes this node, moving the parent node to its next child. */
  public void complete() {
    progress = 1.0f;
    if (parent != null) {
      parent.startNextPhase();
    }
  }

  /** Called during execution on a leaf node to set its progress. */
  public void set(float progress) {
    this.progress = progress;
  }

  /** Returns the overall progress of the root. */
  public float get() {
    Progress node = this;
    while (node.parent != null) {                 // find the root
      node = parent;
    }
    return node.getInternal();
  }

  /** Computes progress in this node. */
  private float getInternal() {
    int phaseCount = phases.size();
    if (phaseCount != 0) {
      float subProgress =
        currentPhase < phaseCount ? phase().getInternal() : 0.0f;
      return progressPerPhase*(currentPhase + subProgress);
    } else {
      return progress;
    }
  }

  public void setStatus(String status) {
    this.status = status;
  }

  public String toString() {
    StringBuffer result = new StringBuffer();
    toString(result);
    return result.toString();
  }

  private void toString(StringBuffer buffer) {
    buffer.append(status);
    if (phases.size() != 0 && currentPhase < phases.size()) {
      buffer.append(" > ");
      phase().toString(buffer);
    }
  }

}
