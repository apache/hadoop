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

import static org.apache.hadoop.hdfs.server.common.Util.now;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.DigestInputStream;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.common.GenerationStamp;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.Text;

/**
 * Contains inner classes for reading or writing the on-disk format for FSImages
 */
public abstract class FSImageFormat {
  private static final Log LOG = FSImage.LOG;
  
  /**
   * A one-shot class responsible for loading an image. The load() function
   * should be called once, after which the getter methods may be used to retrieve
   * information about the image that was loaded, if loading was successful.
   */
  public static class Loader {
    private final Configuration conf;

    /** Set to true once a file has been loaded using this loader. */
    private boolean loaded = false;

    /** The image version of the loaded file */
    private int imgVersion;
    /** The namespace ID of the loaded file */
    private int imgNamespaceID;
    /** The MD5 sum of the loaded file */
    private MD5Hash imgDigest;

    public Loader(Configuration conf) {
      this.conf = conf;
    }

    /**
     * Return the version number of the image that has been loaded.
     * @throws IllegalStateException if load() has not yet been called.
     */
    int getLoadedImageVersion() {
      checkLoaded();
      return imgVersion;
    }
    
    /**
     * Return the MD5 checksum of the image that has been loaded.
     * @throws IllegalStateException if load() has not yet been called.
     */
    MD5Hash getLoadedImageMd5() {
      checkLoaded();
      return imgDigest;
    }

    /**
     * Return the namespace ID of the image that has been loaded.
     * @throws IllegalStateException if load() has not yet been called.
     */
    int getLoadedNamespaceID() {
      checkLoaded();
      return imgNamespaceID;
    }

    /**
     * Throw IllegalStateException if load() has not yet been called.
     */
    private void checkLoaded() {
      if (!loaded) {
        throw new IllegalStateException("Image not yet loaded!");
      }
    }

    /**
     * Throw IllegalStateException if load() has already been called.
     */
    private void checkNotLoaded() {
      if (loaded) {
        throw new IllegalStateException("Image already loaded!");
      }
    }

    void load(File curFile, FSNamesystem targetNamesystem)
      throws IOException
    {
      checkNotLoaded();
      assert curFile != null : "curFile is null";

      long startTime = now();
      FSDirectory fsDir = targetNamesystem.dir;

      //
      // Load in bits
      //
      MessageDigest digester = MD5Hash.getDigester();
      DigestInputStream fin = new DigestInputStream(
           new FileInputStream(curFile), digester);

      DataInputStream in = new DataInputStream(fin);
      try {
        /*
         * Note: Remove any checks for version earlier than 
         * Storage.LAST_UPGRADABLE_LAYOUT_VERSION since we should never get 
         * to here with older images.
         */

        /*
         * TODO we need to change format of the image file
         * it should not contain version and namespace fields
         */
        // read image version: first appeared in version -1
        imgVersion = in.readInt();

        // read namespaceID: first appeared in version -2
        imgNamespaceID = in.readInt();

        // read number of files
        long numFiles = readNumFiles(in);

        // read in the last generation stamp.
        if (imgVersion <= -12) {
          long genstamp = in.readLong();
          targetNamesystem.setGenerationStamp(genstamp); 
        }

        // read compression related info
        FSImageCompression compression;
        if (imgVersion <= -25) {  // -25: 1st version providing compression option
          compression = FSImageCompression.readCompressionHeader(conf, in);
        } else {
          compression = FSImageCompression.createNoopCompression();
        }
        in = compression.unwrapInputStream(fin);

        LOG.info("Loading image file " + curFile + " using " + compression);


        // read file info
        short replication = targetNamesystem.getDefaultReplication();

        LOG.info("Number of files = " + numFiles);

        byte[][] pathComponents;
        byte[][] parentPath = {{}};
        INodeDirectory parentINode = fsDir.rootDir;
        for (long i = 0; i < numFiles; i++) {
          long modificationTime = 0;
          long atime = 0;
          long blockSize = 0;
          pathComponents = FSImageSerialization.readPathComponents(in);
          replication = in.readShort();
          replication = targetNamesystem.adjustReplication(replication);
          modificationTime = in.readLong();
          if (imgVersion <= -17) {
            atime = in.readLong();
          }
          if (imgVersion <= -8) {
            blockSize = in.readLong();
          }
          int numBlocks = in.readInt();
          Block blocks[] = null;

          // for older versions, a blocklist of size 0
          // indicates a directory.
          if ((-9 <= imgVersion && numBlocks > 0) ||
              (imgVersion < -9 && numBlocks >= 0)) {
            blocks = new Block[numBlocks];
            for (int j = 0; j < numBlocks; j++) {
              blocks[j] = new Block();
              if (-14 < imgVersion) {
                blocks[j].set(in.readLong(), in.readLong(), 
                              GenerationStamp.GRANDFATHER_GENERATION_STAMP);
              } else {
                blocks[j].readFields(in);
              }
            }
          }
          // Older versions of HDFS does not store the block size in inode.
          // If the file has more than one block, use the size of the 
          // first block as the blocksize. Otherwise use the default block size.
          //
          if (-8 <= imgVersion && blockSize == 0) {
            if (numBlocks > 1) {
              blockSize = blocks[0].getNumBytes();
            } else {
              long first = ((numBlocks == 1) ? blocks[0].getNumBytes(): 0);
              blockSize = Math.max(targetNamesystem.getDefaultBlockSize(), first);
            }
          }
          
          // get quota only when the node is a directory
          long nsQuota = -1L;
          if (imgVersion <= -16 && blocks == null  && numBlocks == -1) {
            nsQuota = in.readLong();
          }
          long dsQuota = -1L;
          if (imgVersion <= -18 && blocks == null && numBlocks == -1) {
            dsQuota = in.readLong();
          }

          // Read the symlink only when the node is a symlink
          String symlink = "";
          if (imgVersion <= -23 && numBlocks == -2) {
            symlink = Text.readString(in);
          }
          
          PermissionStatus permissions = targetNamesystem.getUpgradePermission();
          if (imgVersion <= -11) {
            permissions = PermissionStatus.read(in);
          }
          
          if (isRoot(pathComponents)) { // it is the root
            // update the root's attributes
            if (nsQuota != -1 || dsQuota != -1) {
              fsDir.rootDir.setQuota(nsQuota, dsQuota);
            }
            fsDir.rootDir.setModificationTime(modificationTime);
            fsDir.rootDir.setPermissionStatus(permissions);
            continue;
          }
          // check if the new inode belongs to the same parent
          if(!isParent(pathComponents, parentPath)) {
            parentINode = null;
            parentPath = getParent(pathComponents);
          }
          // add new inode
          // without propagating modification time to parent
          parentINode = fsDir.addToParent(pathComponents, parentINode, permissions,
                                          blocks, symlink, replication, modificationTime, 
                                          atime, nsQuota, dsQuota, blockSize, false);
        }

        // load datanode info
        this.loadDatanodes(in);

        // load Files Under Construction
        this.loadFilesUnderConstruction(in, targetNamesystem);

        this.loadSecretManagerState(in, targetNamesystem);

      } finally {
        in.close();
      }

      imgDigest = new MD5Hash(digester.digest());
      loaded = true;
      
      LOG.info("Image file of size " + curFile.length() + " loaded in " 
          + (now() - startTime)/1000 + " seconds.");
    }


    private void loadDatanodes(DataInputStream in) throws IOException {
      if (imgVersion > -3) // pre datanode image version
        return;
      if (imgVersion <= -12) {
        return; // new versions do not store the datanodes any more.
      }
      int size = in.readInt();
      for(int i = 0; i < size; i++) {
        // We don't need to add these descriptors any more.
        FSImageSerialization.DatanodeImage.skipOne(in);
      }
    }

    private void loadFilesUnderConstruction(DataInputStream in, 
        FSNamesystem fs) throws IOException {
      FSDirectory fsDir = fs.dir;
      if (imgVersion > -13) // pre lease image version
        return;
      int size = in.readInt();

      LOG.info("Number of files under construction = " + size);

      for (int i = 0; i < size; i++) {
        INodeFileUnderConstruction cons =
          FSImageSerialization.readINodeUnderConstruction(in);

        // verify that file exists in namespace
        String path = cons.getLocalName();
        INode old = fsDir.getFileINode(path);
        if (old == null) {
          throw new IOException("Found lease for non-existent file " + path);
        }
        if (old.isDirectory()) {
          throw new IOException("Found lease for directory " + path);
        }
        INodeFile oldnode = (INodeFile) old;
        fsDir.replaceNode(path, oldnode, cons);
        fs.leaseManager.addLease(cons.getClientName(), path); 
      }
    }

    private void loadSecretManagerState(DataInputStream in, 
        FSNamesystem fs) throws IOException {
      if (imgVersion > -23) {
        //SecretManagerState is not available.
        //This must not happen if security is turned on.
        return; 
      }
      fs.loadSecretManagerState(in);
    }


    private long readNumFiles(DataInputStream in) throws IOException {
      if (imgVersion <= -16) {
        return in.readLong();
      } else {
        return in.readInt();
      }
    }

    private boolean isRoot(byte[][] path) {
      return path.length == 1 &&
        path[0] == null;    
    }

    private boolean isParent(byte[][] path, byte[][] parent) {
      if (path == null || parent == null)
        return false;
      if (parent.length == 0 || path.length != parent.length + 1)
        return false;
      boolean isParent = true;
      for (int i = 0; i < parent.length; i++) {
        isParent = isParent && Arrays.equals(path[i], parent[i]); 
      }
      return isParent;
    }

    /**
     * Return string representing the parent of the given path.
     */
    String getParent(String path) {
      return path.substring(0, path.lastIndexOf(Path.SEPARATOR));
    }
    
    byte[][] getParent(byte[][] path) {
      byte[][] result = new byte[path.length - 1][];
      for (int i = 0; i < result.length; i++) {
        result[i] = new byte[path[i].length];
        System.arraycopy(path[i], 0, result[i], 0, path[i].length);
      }
      return result;
    }
  }
  
  /**
   * A one-shot class responsible for writing an image file.
   * The write() function should be called once, after which the getter
   * functions may be used to retrieve information about the file that was written.
   */
  static class Writer {
    /** Set to true once an image has been written */
    private boolean written = false;
    
    /** The MD5 checksum of the file that was written */
    private MD5Hash writtenDigest;

    static private final byte[] PATH_SEPARATOR = DFSUtil.string2Bytes(Path.SEPARATOR);

    /** @throws IllegalStateException if the instance has not yet written an image */
    private void checkWritten() {
      if (!written) {
        throw new IllegalStateException("FSImageWriter has not written an image");
      }
    }
    
    /** @throws IllegalStateException if the instance has already written an image */
    private void checkNotWritten() {
      if (written) {
        throw new IllegalStateException("FSImageWriter has already written an image");
      }
    }

    /**
     * Return the MD5 checksum of the image file that was saved.
     */
    MD5Hash getWrittenDigest() {
      checkWritten();
      return writtenDigest;
    }

    void write(File newFile,
               FSNamesystem sourceNamesystem,
               FSImageCompression compression)
      throws IOException {
      checkNotWritten();

      FSDirectory fsDir = sourceNamesystem.dir;
      long startTime = now();
      //
      // Write out data
      //
      MessageDigest digester = MD5Hash.getDigester();
      FileOutputStream fout = new FileOutputStream(newFile);
      DigestOutputStream fos = new DigestOutputStream(fout, digester);
      DataOutputStream out = new DataOutputStream(fos);
      try {
        out.writeInt(FSConstants.LAYOUT_VERSION);
        out.writeInt(sourceNamesystem.getFSImage().getNamespaceID()); // TODO bad dependency
        out.writeLong(fsDir.rootDir.numItemsInTree());
        out.writeLong(sourceNamesystem.getGenerationStamp());

        // write compression info and set up compressed stream
        out = compression.writeHeaderAndWrapStream(fos);
        LOG.info("Saving image file " + newFile +
                 " using " + compression);


        byte[] byteStore = new byte[4*FSConstants.MAX_PATH_LENGTH];
        ByteBuffer strbuf = ByteBuffer.wrap(byteStore);
        // save the root
        FSImageSerialization.saveINode2Image(strbuf, fsDir.rootDir, out);
        // save the rest of the nodes
        saveImage(strbuf, 0, fsDir.rootDir, out);
        sourceNamesystem.saveFilesUnderConstruction(out);
        sourceNamesystem.saveSecretManagerState(out);
        strbuf = null;

        out.flush();
        fout.getChannel().force(true);
      } finally {
        out.close();
      }

      written = true;
      // set md5 of the saved image
      writtenDigest = new MD5Hash(digester.digest());

      LOG.info("Image file of size " + newFile.length() + " saved in " 
          + (now() - startTime)/1000 + " seconds.");
    }

    /**
     * Save file tree image starting from the given root.
     * This is a recursive procedure, which first saves all children of
     * a current directory and then moves inside the sub-directories.
     */
    private static void saveImage(ByteBuffer parentPrefix,
                                  int prefixLength,
                                  INodeDirectory current,
                                  DataOutputStream out) throws IOException {
      int newPrefixLength = prefixLength;
      if (current.getChildrenRaw() == null)
        return;
      for(INode child : current.getChildren()) {
        // print all children first
        parentPrefix.position(prefixLength);
        parentPrefix.put(PATH_SEPARATOR).put(child.getLocalNameBytes());
        FSImageSerialization.saveINode2Image(parentPrefix, child, out);
      }
      for(INode child : current.getChildren()) {
        if(!child.isDirectory())
          continue;
        parentPrefix.position(prefixLength);
        parentPrefix.put(PATH_SEPARATOR).put(child.getLocalNameBytes());
        newPrefixLength = parentPrefix.position();
        saveImage(parentPrefix, newPrefixLength, (INodeDirectory)child, out);
      }
      parentPrefix.position(prefixLength);
    }
  }
}
