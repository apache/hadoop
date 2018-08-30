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

package org.apache.hadoop.fs.s3a.s3guard;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Constants;
import org.apache.hadoop.fs.s3a.Tristate;

/**
 * Defines methods for translating between domain model objects and their
 * representations in the DynamoDB schema.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
final class PathMetadataDynamoDBTranslation {

  /** The HASH key name of each item. */
  @VisibleForTesting
  static final String PARENT = "parent";
  /** The RANGE key name of each item. */
  @VisibleForTesting
  static final String CHILD = "child";
  @VisibleForTesting
  static final String IS_DIR = "is_dir";
  @VisibleForTesting
  static final String MOD_TIME = "mod_time";
  @VisibleForTesting
  static final String FILE_LENGTH = "file_length";
  @VisibleForTesting
  static final String BLOCK_SIZE = "block_size";
  static final String IS_DELETED = "is_deleted";
  static final String IS_AUTHORITATIVE = "is_authoritative";

  /** Table version field {@value} in version marker item. */
  @VisibleForTesting
  static final String TABLE_VERSION = "table_version";

  /** Table creation timestampfield {@value} in version marker item. */
  @VisibleForTesting
  static final String TABLE_CREATED = "table_created";

  /** The version marker field is invalid. */
  static final String E_NOT_VERSION_MARKER = "Not a version marker: ";

  /**
   * Returns the key schema for the DynamoDB table.
   *
   * @return DynamoDB key schema
   */
  static Collection<KeySchemaElement> keySchema() {
    return Arrays.asList(
        new KeySchemaElement(PARENT, KeyType.HASH),
        new KeySchemaElement(CHILD, KeyType.RANGE));
  }

  /**
   * Returns the attribute definitions for the DynamoDB table.
   *
   * @return DynamoDB attribute definitions
   */
  static Collection<AttributeDefinition> attributeDefinitions() {
    return Arrays.asList(
        new AttributeDefinition(PARENT, ScalarAttributeType.S),
        new AttributeDefinition(CHILD, ScalarAttributeType.S));
  }

  /**
   * Converts a DynamoDB item to a {@link DDBPathMetadata}.
   *
   * @param item DynamoDB item to convert
   * @return {@code item} converted to a {@link DDBPathMetadata}
   */
  static DDBPathMetadata itemToPathMetadata(Item item, String username)
      throws IOException {
    return itemToPathMetadata(item, username, false);
  }

  /**
   * Converts a DynamoDB item to a {@link DDBPathMetadata}.
   * Can ignore {@code IS_AUTHORITATIVE} flag if {@code ignoreIsAuthFlag} is
   * true.
   *
   * @param item DynamoDB item to convert
   * @param ignoreIsAuthFlag if true, ignore the authoritative flag on item
   * @return {@code item} converted to a {@link DDBPathMetadata}
   */
  static DDBPathMetadata itemToPathMetadata(Item item, String username,
      boolean ignoreIsAuthFlag)
      throws IOException {
    if (item == null) {
      return null;
    }

    String parentStr = item.getString(PARENT);
    Preconditions.checkNotNull(parentStr, "No parent entry in item %s", item);
    String childStr = item.getString(CHILD);
    Preconditions.checkNotNull(childStr, "No child entry in item %s", item);

    // Skip table version markers, which are only non-absolute paths stored.
    Path rawPath = new Path(parentStr, childStr);
    if (!rawPath.isAbsoluteAndSchemeAuthorityNull()) {
      return null;
    }

    Path parent = new Path(Constants.FS_S3A + ":/" + parentStr + "/");
    Path path = new Path(parent, childStr);

    boolean isDir = item.hasAttribute(IS_DIR) && item.getBoolean(IS_DIR);
    boolean isAuthoritativeDir = false;
    final FileStatus fileStatus;
    if (isDir) {
      if (!ignoreIsAuthFlag) {
        isAuthoritativeDir = item.hasAttribute(IS_AUTHORITATIVE)
            && item.getBoolean(IS_AUTHORITATIVE);
      }
      fileStatus = DynamoDBMetadataStore.makeDirStatus(path, username);
    } else {
      long len = item.hasAttribute(FILE_LENGTH) ? item.getLong(FILE_LENGTH) : 0;
      long modTime = item.hasAttribute(MOD_TIME) ? item.getLong(MOD_TIME) : 0;
      long block = item.hasAttribute(BLOCK_SIZE) ? item.getLong(BLOCK_SIZE) : 0;
      fileStatus = new FileStatus(len, false, 1, block, modTime, 0, null,
          username, username, path);
    }
    boolean isDeleted =
        item.hasAttribute(IS_DELETED) && item.getBoolean(IS_DELETED);

    return new DDBPathMetadata(fileStatus, Tristate.UNKNOWN, isDeleted,
        isAuthoritativeDir);
  }

  /**
   * Converts a {@link DDBPathMetadata} to a DynamoDB item.
   *
   * @param meta {@link DDBPathMetadata} to convert
   * @return {@code meta} converted to DynamoDB item
   */
  static Item pathMetadataToItem(DDBPathMetadata meta) {
    return pathMetadataToItem(meta, false);
  }

  /**
   * Converts a {@link DDBPathMetadata} to a DynamoDB item.
   *
   * Can ignore {@code IS_AUTHORITATIVE} flag if {@code ignoreIsAuthFlag} is
   * true.
   *
   * @param meta {@link DDBPathMetadata} to convert
   * @param ignoreIsAuthFlag if true, ignore the authoritative flag on item
   * @return {@code meta} converted to DynamoDB item
   */
  static Item pathMetadataToItem(DDBPathMetadata meta,
      boolean ignoreIsAuthFlag) {
    Preconditions.checkNotNull(meta);
    final FileStatus status = meta.getFileStatus();
    final Item item = new Item().withPrimaryKey(pathToKey(status.getPath()));
    if (status.isDirectory()) {
      item.withBoolean(IS_DIR, true);
      if (!ignoreIsAuthFlag) {
        item.withBoolean(IS_AUTHORITATIVE, meta.isAuthoritativeDir());
      }
    } else {
      item.withLong(FILE_LENGTH, status.getLen())
          .withLong(MOD_TIME, status.getModificationTime())
          .withLong(BLOCK_SIZE, status.getBlockSize());
    }
    item.withBoolean(IS_DELETED, meta.isDeleted());
    return item;
  }

  /**
   * The version marker has a primary key whose PARENT is {@code name};
   * this MUST NOT be a value which represents an absolute path.
   * @param name name of the version marker
   * @param version version number
   * @param timestamp creation timestamp
   * @return an item representing a version marker.
   */
  static Item createVersionMarker(String name, int version, long timestamp) {
    return new Item().withPrimaryKey(createVersionMarkerPrimaryKey(name))
        .withInt(TABLE_VERSION, version)
        .withLong(TABLE_CREATED, timestamp);
  }

  /**
   * Create the primary key of the version marker.
   * @param name key name
   * @return the key to use when registering or resolving version markers
   */
  static PrimaryKey createVersionMarkerPrimaryKey(String name) {
    return new PrimaryKey(PARENT, name, CHILD, name);
  }

  /**
   * Extract the version from a version marker item.
   * @param marker version marker item
   * @return the extracted version field
   * @throws IOException if the item is not a version marker
   */
  static int extractVersionFromMarker(Item marker) throws IOException {
    if (marker.hasAttribute(TABLE_VERSION)) {
      return marker.getInt(TABLE_VERSION);
    } else {
      throw new IOException(E_NOT_VERSION_MARKER + marker);
    }
  }

  /**
   * Extract the creation time, if present.
   * @param marker version marker item
   * @return the creation time, or null
   * @throws IOException if the item is not a version marker
   */
  static Long extractCreationTimeFromMarker(Item marker) throws IOException {
    if (marker.hasAttribute(TABLE_CREATED)) {
      return marker.getLong(TABLE_CREATED);
    } else {
      return null;
    }
  }

  /**
   * Converts a collection {@link DDBPathMetadata} to a collection DynamoDB
   * items.
   *
   * @see #pathMetadataToItem(DDBPathMetadata)
   */
  static Item[] pathMetadataToItem(Collection<DDBPathMetadata> metas) {
    if (metas == null) {
      return null;
    }

    final Item[] items = new Item[metas.size()];
    int i = 0;
    for (DDBPathMetadata meta : metas) {
      items[i++] = pathMetadataToItem(meta);
    }
    return items;
  }

  /**
   * Converts a {@link Path} to a DynamoDB equality condition on that path as
   * parent, suitable for querying all direct children of the path.
   *
   * @param path the path; can not be null
   * @return DynamoDB equality condition on {@code path} as parent
   */
  static KeyAttribute pathToParentKeyAttribute(Path path) {
    return new KeyAttribute(PARENT, pathToParentKey(path));
  }

  /**
   * e.g. {@code pathToParentKey(s3a://bucket/path/a) -> /bucket/path/a}
   * @param path path to convert
   * @return string for parent key
   */
  static String pathToParentKey(Path path) {
    Preconditions.checkNotNull(path);
    Preconditions.checkArgument(path.isUriPathAbsolute(), "Path not absolute");
    URI uri = path.toUri();
    String bucket = uri.getHost();
    Preconditions.checkArgument(!StringUtils.isEmpty(bucket),
        "Path missing bucket");
    String pKey = "/" + bucket + uri.getPath();

    // Strip trailing slash
    if (pKey.endsWith("/")) {
      pKey = pKey.substring(0, pKey.length() - 1);
    }
    return pKey;
  }

  /**
   * Converts a {@link Path} to a DynamoDB key, suitable for getting the item
   * matching the path.
   *
   * @param path the path; can not be null
   * @return DynamoDB key for item matching {@code path}
   */
  static PrimaryKey pathToKey(Path path) {
    Preconditions.checkArgument(!path.isRoot(),
        "Root path is not mapped to any PrimaryKey");
    return new PrimaryKey(PARENT, pathToParentKey(path.getParent()), CHILD,
        path.getName());
  }

  /**
   * Converts a collection of {@link Path} to a collection of DynamoDB keys.
   *
   * @see #pathToKey(Path)
   */
  static PrimaryKey[] pathToKey(Collection<Path> paths) {
    if (paths == null) {
      return null;
    }

    final PrimaryKey[] keys = new PrimaryKey[paths.size()];
    int i = 0;
    for (Path p : paths) {
      keys[i++] = pathToKey(p);
    }
    return keys;
  }

  /**
   * There is no need to instantiate this class.
   */
  private PathMetadataDynamoDBTranslation() {
  }

  static List<DDBPathMetadata> pathMetaToDDBPathMeta(
      Collection<PathMetadata> pathMetadatas) {
    return pathMetadatas.stream().map(p -> new DDBPathMetadata(p))
        .collect(Collectors.toList());
  }

}
