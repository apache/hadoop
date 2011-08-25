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
package org.apache.hadoop.hdfs.tools.offlineEditsViewer;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * An EditsLoader can read a Hadoop EditLog file  and walk over its
 * structure using the supplied EditsVisitor.
 *
 * Each implementation of EditsLoader is designed to rapidly process an
 * edits log file.  As long as minor changes are made from one layout version
 * to another, it is acceptable to tweak one implementation to read the next.
 * However, if the layout version changes enough that it would make a
 * processor slow or difficult to read, another processor should be created.
 * This allows each processor to quickly read an edits log without getting
 * bogged down in dealing with significant differences between layout versions.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
interface EditsLoader {

  /**
   * Loads the edits file
   */
  public void loadEdits() throws IOException;

  /**
   * Can this processor handle the specified version of EditLog file?
   *
   * @param version EditLog version file
   * @return True if this instance can process the file
   */
  public boolean canLoadVersion(int version);

  /**
   * Factory for obtaining version of edits log loader that can read
   * a particular edits log format.
   */
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public class LoaderFactory {
    // Java doesn't support static methods on interfaces, which necessitates
    // this factory class

    /**
     * Create an edits log loader, at this point we only have one,
     * we might need to add more later
     *
     * @param v an instance of EditsVisitor (binary, XML etc.)
     * @return EditsLoader that can interpret specified version, or null
     */
    static public EditsLoader getLoader(EditsVisitor v) {
      return new EditsLoaderCurrent(v);
    }
  }
}
