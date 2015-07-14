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

import com.google.common.collect.Lists;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hdfs.inotify.Event;
import org.apache.hadoop.hdfs.inotify.EventBatch;
import org.apache.hadoop.hdfs.protocol.Block;

import java.util.List;

/**
 * Translates from edit log ops to inotify events.
 */
@InterfaceAudience.Private
public class InotifyFSEditLogOpTranslator {

  private static long getSize(FSEditLogOp.AddCloseOp acOp) {
    long size = 0;
    for (Block b : acOp.getBlocks()) {
      size += b.getNumBytes();
    }
    return size;
  }

  public static EventBatch translate(FSEditLogOp op) {
    switch(op.opCode) {
    case OP_ADD:
      FSEditLogOp.AddOp addOp = (FSEditLogOp.AddOp) op;
      if (addOp.blocks.length == 0) { // create
        return new EventBatch(op.txid,
            new Event[] { new Event.CreateEvent.Builder().path(addOp.path)
            .ctime(addOp.atime)
            .replication(addOp.replication)
            .ownerName(addOp.permissions.getUserName())
            .groupName(addOp.permissions.getGroupName())
            .perms(addOp.permissions.getPermission())
            .overwrite(addOp.overwrite)
            .defaultBlockSize(addOp.blockSize)
            .iNodeType(Event.CreateEvent.INodeType.FILE).build() });
      } else { // append
        return new EventBatch(op.txid,
            new Event[]{new Event.AppendEvent.Builder()
                .path(addOp.path)
                .build()});
      }
    case OP_CLOSE:
      FSEditLogOp.CloseOp cOp = (FSEditLogOp.CloseOp) op;
      return new EventBatch(op.txid, new Event[] {
          new Event.CloseEvent(cOp.path, getSize(cOp), cOp.mtime) });
    case OP_APPEND:
      FSEditLogOp.AppendOp appendOp = (FSEditLogOp.AppendOp) op;
      return new EventBatch(op.txid, new Event[] {new Event.AppendEvent
          .Builder().path(appendOp.path).newBlock(appendOp.newBlock).build()});
    case OP_SET_REPLICATION:
      FSEditLogOp.SetReplicationOp setRepOp = (FSEditLogOp.SetReplicationOp) op;
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.REPLICATION)
          .path(setRepOp.path)
          .replication(setRepOp.replication).build() });
    case OP_CONCAT_DELETE:
      FSEditLogOp.ConcatDeleteOp cdOp = (FSEditLogOp.ConcatDeleteOp) op;
      List<Event> events = Lists.newArrayList();
      events.add(new Event.AppendEvent.Builder()
          .path(cdOp.trg)
          .build());
      for (String src : cdOp.srcs) {
        events.add(new Event.UnlinkEvent.Builder()
          .path(src)
          .timestamp(cdOp.timestamp)
          .build());
      }
      events.add(new Event.CloseEvent(cdOp.trg, -1, cdOp.timestamp));
      return new EventBatch(op.txid, events.toArray(new Event[0]));
    case OP_RENAME_OLD:
      FSEditLogOp.RenameOldOp rnOpOld = (FSEditLogOp.RenameOldOp) op;
      return new EventBatch(op.txid, new Event[] {
          new Event.RenameEvent.Builder()
              .srcPath(rnOpOld.src)
              .dstPath(rnOpOld.dst)
              .timestamp(rnOpOld.timestamp)
              .build() });
    case OP_RENAME:
      FSEditLogOp.RenameOp rnOp = (FSEditLogOp.RenameOp) op;
      return new EventBatch(op.txid, new Event[] {
          new Event.RenameEvent.Builder()
            .srcPath(rnOp.src)
            .dstPath(rnOp.dst)
            .timestamp(rnOp.timestamp)
            .build() });
    case OP_DELETE:
      FSEditLogOp.DeleteOp delOp = (FSEditLogOp.DeleteOp) op;
      return new EventBatch(op.txid, new Event[] {
          new Event.UnlinkEvent.Builder()
            .path(delOp.path)
            .timestamp(delOp.timestamp)
            .build() });
    case OP_MKDIR:
      FSEditLogOp.MkdirOp mkOp = (FSEditLogOp.MkdirOp) op;
      return new EventBatch(op.txid,
        new Event[] { new Event.CreateEvent.Builder().path(mkOp.path)
          .ctime(mkOp.timestamp)
          .ownerName(mkOp.permissions.getUserName())
          .groupName(mkOp.permissions.getGroupName())
          .perms(mkOp.permissions.getPermission())
          .iNodeType(Event.CreateEvent.INodeType.DIRECTORY).build() });
    case OP_SET_PERMISSIONS:
      FSEditLogOp.SetPermissionsOp permOp = (FSEditLogOp.SetPermissionsOp) op;
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.PERMS)
          .path(permOp.src)
          .perms(permOp.permissions).build() });
    case OP_SET_OWNER:
      FSEditLogOp.SetOwnerOp ownOp = (FSEditLogOp.SetOwnerOp) op;
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.OWNER)
          .path(ownOp.src)
          .ownerName(ownOp.username).groupName(ownOp.groupname).build() });
    case OP_TIMES:
      FSEditLogOp.TimesOp timesOp = (FSEditLogOp.TimesOp) op;
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.TIMES)
          .path(timesOp.path)
          .atime(timesOp.atime).mtime(timesOp.mtime).build() });
    case OP_SYMLINK:
      FSEditLogOp.SymlinkOp symOp = (FSEditLogOp.SymlinkOp) op;
      return new EventBatch(op.txid,
        new Event[] { new Event.CreateEvent.Builder().path(symOp.path)
          .ctime(symOp.atime)
          .ownerName(symOp.permissionStatus.getUserName())
          .groupName(symOp.permissionStatus.getGroupName())
          .perms(symOp.permissionStatus.getPermission())
          .symlinkTarget(symOp.value)
          .iNodeType(Event.CreateEvent.INodeType.SYMLINK).build() });
    case OP_REMOVE_XATTR:
      FSEditLogOp.RemoveXAttrOp rxOp = (FSEditLogOp.RemoveXAttrOp) op;
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.XATTRS)
          .path(rxOp.src)
          .xAttrs(rxOp.xAttrs)
          .xAttrsRemoved(true).build() });
    case OP_SET_XATTR:
      FSEditLogOp.SetXAttrOp sxOp = (FSEditLogOp.SetXAttrOp) op;
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.XATTRS)
          .path(sxOp.src)
          .xAttrs(sxOp.xAttrs)
          .xAttrsRemoved(false).build() });
    case OP_SET_ACL:
      FSEditLogOp.SetAclOp saOp = (FSEditLogOp.SetAclOp) op;
      return new EventBatch(op.txid,
        new Event[] { new Event.MetadataUpdateEvent.Builder()
          .metadataType(Event.MetadataUpdateEvent.MetadataType.ACLS)
          .path(saOp.src)
          .acls(saOp.aclEntries).build() });
    case OP_TRUNCATE:
      FSEditLogOp.TruncateOp tOp = (FSEditLogOp.TruncateOp) op;
      return new EventBatch(op.txid, new Event[] {
          new Event.TruncateEvent(tOp.src, tOp.newLength, tOp.timestamp) });
    default:
      return null;
    }
  }
}
