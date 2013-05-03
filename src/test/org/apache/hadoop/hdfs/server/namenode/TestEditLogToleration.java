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

package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.assertTrue;

import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.server.namenode.FSImage.NameNodeFile;
import org.apache.hadoop.metrics2.util.MBeans;
import org.apache.log4j.Level;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test the edit log toleration feature
 * which allows namenode to tolerate edit log corruption.
 * The corruption could possibly be edit file truncation, extra padding,
 * truncate-and-then-pad, etc.
 */
public class TestEditLogToleration {
  {
    ((Log4JLogger)FSEditLog.LOG).getLogger().setLevel(Level.ALL);
    ((Log4JLogger)LogFactory.getLog(MBeans.class)).getLogger().setLevel(Level.OFF);
  }

  private static final Log LOG = LogFactory.getLog(TestEditLogToleration.class);

  private static final int TOLERATION_LENGTH = 1024;
  private static final byte[] PADS = {0, FSEditLog.OP_INVALID};
  
  private static final int LOOP = 8;
  private static final Random RANDOM = new Random(); 

  /** Base class for modifying edit log to simulate file corruption. */
  static abstract class EditFileModifier {
    void log(File f) {
      LOG.info(getClass().getSimpleName() + ": length=" + f.length() + ", f=" + f);
    }

    /** Modify the edit file. */
    abstract void modify(File editFile) throws IOException;

    /** Is the modification tolerable? */
    abstract boolean isTolerable();
  }

  /** NullModifier does not change the file at all for testing normal case. */
  static class NullModifier extends EditFileModifier {
    @Override
    void modify(File f) throws IOException {log(f);}
    @Override
    boolean isTolerable() {return true;}
  }

  /** Truncate the edit file. */
  static class Truncating extends EditFileModifier {
    final int truncationLength;

    /**
     * @param truncationLength truncate the file by this length
     */
    Truncating(int truncationLength) {
      this.truncationLength = truncationLength;
    }

    @Override
    void modify(File f) throws IOException {
      // truncate the file
      log(f);
      final RandomAccessFile raf = new RandomAccessFile(f, "rw");
      raf.setLength(f.length() - truncationLength);
      raf.close();
      LOG.info(getClass().getSimpleName() + ": new length=" + f.length()
          + ", truncationLength=" + truncationLength);
    }
    
    @Override
    boolean isTolerable() {
      // Because the editlog is truncated, only the last remaining
      // transaction could be incomplete and hence corrupt. Since
      // TOLERATION_LENGTH is much larger than any transaction length,
      // truncation is tolerable corruption.
      return true;
    }
  }
  
  /**
   * Add padding to the edit file for simulating normal or error padding.
   * The padding could be the same padding (i.e. 0 or OP_INVALID) used
   * in edit log preallocation (see EditLogFileOutputStream.preallcate()),
   * or some other values.
   * 
   * This modifier changes the editlog to:
   * Add (corruptionLeangth - 1) padding bytes, followed by a corrupt byte
   * followed (paddinLength) padding bytes.
   * 
   * |- valid bytes -|- padding bytes -|- corrupt byte -|-- padding bytes --|
   */
  static class Padding extends EditFileModifier {
    static final byte[] PAD_BUFFER = new byte[4096];
    final int corruptionLength;
    final int paddingLength;
    final byte pad;

    /**
     * @param corruptionLength corrupt bytes
     * @param paddingLength number padding bytes added
     * @param pad what byte is used for padding
     */
    Padding(int corruptionLength, int paddingLength, byte pad) {
      this.corruptionLength = corruptionLength;
      this.paddingLength = paddingLength;
      this.pad = pad;
    }

    @Override
    void modify(File f) throws IOException {
      log(f);

      // Append padding
      final RandomAccessFile raf = new RandomAccessFile(f, "rw");
      raf.seek(f.length());
      if (corruptionLength > 0) {
        pad(raf, pad, corruptionLength - 1);
        raf.write(0xAB);
      }
      pad(raf, pad, paddingLength);
      raf.close();
      LOG.info(getClass().getSimpleName() + ": new length=" + f.length()
          + ", corruptionLength=" + corruptionLength
          + ", paddingLength=" + paddingLength);
    }
    
    @Override
    boolean isTolerable() {
      return corruptionLength <= TOLERATION_LENGTH;
    }

    private static void pad(final DataOutput out, final byte pad, int length
        ) throws IOException {
      Arrays.fill(PAD_BUFFER, pad);
      for(; length > 0; ) {
        final int n = length < PAD_BUFFER.length? length : PAD_BUFFER.length;
        out.write(PAD_BUFFER, 0, n);
        length -= n;
      }
    }
  }
  
  /**
   * Chain several modifier together for simulating behavior like
   * truncate-and-then-pad.
   */
  static class ChainModifier extends EditFileModifier {
    final List<EditFileModifier> modifers;

    ChainModifier(EditFileModifier ... modifiers) {
      this.modifers = Arrays.asList(modifiers);
    }

    @Override
    void modify(File editFile) throws IOException {
      for(EditFileModifier m : modifers) {
        m.modify(editFile);
      }
    }

    @Override
    boolean isTolerable() {
      // Note in some cases tolerable combination could result in intolerable
      // modifier. Similarly intolerable combination could also result in
      // tolerable modifier.
      // The following tests ensure that such combination are not possible by
      // choosing appropriate modifier properties
      for(EditFileModifier m : modifers) {
        if (!m.isTolerable()) {
          return false;
        }
      }
      return true;
    }
  }

  void runTest(EditFileModifier modifier) throws IOException {
    //set toleration length
    final Configuration conf = new Configuration();
    conf.setInt(DFSConfigKeys.DFS_NAMENODE_EDITS_TOLERATION_LENGTH_KEY, TOLERATION_LENGTH);

    final MiniDFSCluster cluster = new MiniDFSCluster(conf, 0, true, null);
    try {
      cluster.waitActive();

      //add a few transactions and then shutdown namenode.
      final FileSystem fs = cluster.getFileSystem();
      fs.mkdirs(new Path("/user/foo"));
      fs.mkdirs(new Path("/user/bar"));
      cluster.shutdownNameNode();

      //modify edit files
      for(File dir : FSNamesystem.getNamespaceEditsDirs(conf)) {
        final File editFile  = new File(new File(dir, "current"),
            NameNodeFile.EDITS.getName());
        assertTrue("Should exist: " + editFile, editFile.exists());

        modifier.modify(editFile);
      }

      try {
        //restart namenode.
        cluster.restartNameNode();
        
        //No exception: the modification must be tolerable.
        Assert.assertTrue(modifier.isTolerable());
      } catch (IOException e) {
        //Got an exception: the modification must be intolerable.
        LOG.info("Got an exception", e);
        Assert.assertFalse(modifier.isTolerable());
      }
    } finally {
      cluster.shutdown();
    }
  }

  @Test
  public void testNoModification() throws IOException {
    runTest(new NullModifier());
  }

  /** Test truncating some bytes. */
  @Test
  public void testTruncatedEditLog() throws IOException {
    for(int i = 0; i < LOOP; i++) {
      final int truncation = RANDOM.nextInt(100);
      runTest(new Truncating(truncation));
    }
  }

  /** Test padding some bytes with no corruption. */
  @Test
  public void testNormalPaddedEditLog() throws IOException {
    for(int i = 0; i < LOOP/2; i++) {
      //zero corruption
      final int padding = RANDOM.nextInt(2*TOLERATION_LENGTH);
      final byte pad = PADS[RANDOM.nextInt(PADS.length)];

      final Padding p = new Padding(0, padding, pad);
      Assert.assertTrue(p.isTolerable());
      runTest(p);
    }
  }

  /** Test corruption and padding with corruption length <= toleration length. */
  @Test
  public void testTolerableErrorPaddedEditLog() throws IOException {
    for(int i = 0; i < LOOP; i++) {
      //0 < corruption <= TOLERATION_LENGTH
      final int corruption = RANDOM.nextInt(TOLERATION_LENGTH) + 1;
      Assert.assertTrue(corruption > 0);
      Assert.assertTrue(corruption <= TOLERATION_LENGTH);
      final int padding = RANDOM.nextInt(2*TOLERATION_LENGTH);
      final byte pad = PADS[RANDOM.nextInt(PADS.length)];

      final Padding p = new Padding(corruption, padding, pad);
      Assert.assertTrue(p.isTolerable());
      runTest(p);
    }
  }

  /** Test corruption and padding with corruption length > toleration length. */
  @Test
  public void testIntolerableErrorPaddedEditLog() throws IOException {
    for(int i = 0; i < LOOP; i++) {
      //corruption > TOLERATION_LENGTH
      final int corruption = RANDOM.nextInt(TOLERATION_LENGTH) + TOLERATION_LENGTH + 1;
      Assert.assertTrue(corruption > TOLERATION_LENGTH);
      final int padding = RANDOM.nextInt(2*TOLERATION_LENGTH);
      final byte pad = PADS[RANDOM.nextInt(PADS.length)];

      final Padding p = new Padding(corruption, padding, pad);
      Assert.assertFalse(p.isTolerable());
      runTest(p);
    }
  }

  /** Test truncate and then pad. */
  @Test
  public void testTruncateAndNormalPaddedEditLog() throws IOException {
    for(int i = 0; i < LOOP; i++) {
      //truncation
      final int truncation = RANDOM.nextInt(100);
      final Truncating t = new Truncating(truncation);

      //padding with zero corruption
      final int padding = RANDOM.nextInt(2*TOLERATION_LENGTH);
      final byte pad = PADS[RANDOM.nextInt(PADS.length)];
      final Padding p = new Padding(0, padding, pad);

      //chain: truncate and then pad
      final ChainModifier chain = new ChainModifier(t, p);
      Assert.assertTrue(chain.isTolerable());
      runTest(chain);
    }
  }

  /** Test truncate and then pad with tolerable corruption. */
  @Test
  public void testTruncateAndTolerableErrorPaddedEditLog() throws IOException {
    for(int i = 0; i < LOOP; i++) {
      //truncation
      final int truncation = RANDOM.nextInt(100);
      final Truncating t = new Truncating(truncation);

      // padding with tolerable corruption (0 < corruption <= TOLERATION_LENGTH/2)
      // Note the choice of padding here ensures that the corruption is tolerable
      final int corruption = RANDOM.nextInt(TOLERATION_LENGTH >> 1) + 1;
      Assert.assertTrue(corruption > 0);
      Assert.assertTrue(corruption <= TOLERATION_LENGTH/2);
      final int padding = RANDOM.nextInt(2*TOLERATION_LENGTH);
      final byte pad = PADS[RANDOM.nextInt(PADS.length)];
      final Padding p = new Padding(corruption, padding, pad);

      //chain: truncate and then pad
      final ChainModifier chain = new ChainModifier(t, p);
      Assert.assertTrue(chain.isTolerable());
      runTest(chain);
    }
  }

  /** Test truncate and then pad with intolerable corruption. */
  @Test
  public void testTruncateAndIntolerableErrorPaddedEditLog() throws IOException {
    for(int i = 0; i < LOOP; i++) {
      //truncation
      final int truncation = RANDOM.nextInt(100);
      final Truncating t = new Truncating(truncation);

      //padding with intolerable corruption (corruption > TOLERATION_LENGTH)
      final int corruption = RANDOM.nextInt(TOLERATION_LENGTH) + TOLERATION_LENGTH + 1;
      Assert.assertTrue(corruption > TOLERATION_LENGTH);
      final int padding = RANDOM.nextInt(2*TOLERATION_LENGTH);
      final byte pad = PADS[RANDOM.nextInt(PADS.length)];
      final Padding p = new Padding(corruption, padding, pad);

      //chain: truncate and then pad
      final ChainModifier chain = new ChainModifier(t, p);
      Assert.assertFalse(chain.isTolerable());
      runTest(chain);
    }
  }
}
