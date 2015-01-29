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

package org.apache.hadoop.hdfs.server.datanode.extdataset;

import org.apache.hadoop.hdfs.server.datanode.Replica;
import org.apache.hadoop.hdfs.server.datanode.ReplicaInPipelineInterface;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsDatasetSpi;
import org.apache.hadoop.hdfs.server.datanode.fsdataset.FsVolumeSpi;
import org.junit.Test;

/**
 * Tests the ability to create external FsDatasetSpi implementations.
 *
 * The purpose of this suite of tests is to ensure that it is possible to
 * construct subclasses of FsDatasetSpi outside the Hadoop tree
 * (specifically, outside of the org.apache.hadoop.hdfs.server.datanode
 * package).  This consists of creating subclasses of the two key classes
 * (FsDatasetSpi and FsVolumeSpi) *and* instances or subclasses of any
 * classes/interfaces their methods need to produce.  If methods are added
 * to or changed in any superclasses, or if constructors of other classes
 * are changed, this package will fail to compile.  In fixing this
 * compilation error, any new class dependencies should receive the same
 * treatment.
 *
 * It is worth noting what these tests do *not* accomplish.  Just as
 * important as being able to produce instances of the appropriate classes
 * is being able to access all necessary methods on those classes as well
 * as on any additional classes accepted as inputs to FsDatasetSpi's
 * methods.  It wouldn't be correct to mandate all methods be public, as
 * that would defeat encapsulation.  Moreover, there is no natural
 * mechanism that would prevent a manually-constructed list of methods
 * from becoming stale.  Rather than creating tests with no clear means of
 * maintaining them, this problem is left unsolved for now.
 *
 * Lastly, though merely compiling this package should signal success,
 * explicit testInstantiate* unit tests are included below so as to have a
 * tangible means of referring to each case.
 */
public class TestExternalDataset {

  /**
   * Tests instantiating an FsDatasetSpi subclass.
   */
  @Test
  public void testInstantiateDatasetImpl() throws Throwable {
    FsDatasetSpi<?> inst = new ExternalDatasetImpl();
  }

  /**
   * Tests instantiating a Replica subclass.
   */
  @Test
  public void testIntantiateExternalReplica() throws Throwable {
    Replica inst = new ExternalReplica();
  }

  /**
   * Tests instantiating a ReplicaInPipelineInterface subclass.
   */
  @Test
  public void testInstantiateReplicaInPipeline() throws Throwable {
    ReplicaInPipelineInterface inst = new ExternalReplicaInPipeline();
  }

  /**
   * Tests instantiating an FsVolumeSpi subclass.
   */
  @Test
  public void testInstantiateVolumeImpl() throws Throwable {
    FsVolumeSpi inst = new ExternalVolumeImpl();
  }
}
