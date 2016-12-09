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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.KeyAttribute;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.S3AFileStatus;

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
   * Converts a DynamoDB item to a {@link PathMetadata}.
   *
   * @param item DynamoDB item to convert
   * @return {@code item} converted to a {@link PathMetadata}
   */
  static PathMetadata itemToPathMetadata(URI s3aUri, Item item, String username)
      throws IOException {
    if (item == null) {
      return null;
    }

    Path path = new Path(item.getString(PARENT), item.getString(CHILD));
    if (!path.isAbsoluteAndSchemeAuthorityNull()) {
      return null;
    }

    path = path.makeQualified(s3aUri, null);
    boolean isDir = item.hasAttribute(IS_DIR) && item.getBoolean(IS_DIR);
    final FileStatus fileStatus;
    if (isDir) {
      fileStatus = new S3AFileStatus(true, path, username);
    } else {
      long len = item.hasAttribute(FILE_LENGTH) ? item.getLong(FILE_LENGTH) : 0;
      long modTime = item.hasAttribute(MOD_TIME) ? item.getLong(MOD_TIME) : 0;
      long block = item.hasAttribute(BLOCK_SIZE) ? item.getLong(BLOCK_SIZE) : 0;
      fileStatus = new S3AFileStatus(len, modTime, path, block, username);
    }

    return new PathMetadata(fileStatus);
  }

  /**
   * Converts a {@link PathMetadata} to a DynamoDB item.
   *
   * @param meta {@link PathMetadata} to convert
   * @return {@code meta} converted to DynamoDB item
   */
  static Item pathMetadataToItem(PathMetadata meta) {
    Preconditions.checkNotNull(meta);
    assert meta.getFileStatus() instanceof S3AFileStatus;
    final S3AFileStatus status = (S3AFileStatus) meta.getFileStatus();
    final Item item = new Item().withPrimaryKey(pathToKey(status.getPath()));
    if (status.isDirectory()) {
      item.withBoolean(IS_DIR, true);
    } else {
      item.withLong(FILE_LENGTH, status.getLen())
          .withLong(MOD_TIME, status.getModificationTime())
          .withLong(BLOCK_SIZE, status.getBlockSize());
    }

    return item;
  }

  /**
   * Converts a collection {@link PathMetadata} to a collection DynamoDB items.
   *
   * @see #pathMetadataToItem(PathMetadata)
   */
  static Collection<Item> pathMetadataToItem(Collection<PathMetadata> metas) {
    final List<Item> items = new ArrayList<>(metas.size());
    for (PathMetadata meta : metas) {
      items.add(pathMetadataToItem(meta));
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
    removeSchemeAndAuthority(path);
    return new KeyAttribute(PARENT, path.toUri().getPath());
  }

  /**
   * Converts a {@link Path} to a DynamoDB key, suitable for getting the item
   * matching the path.
   *
   * @param path the path; can not be null
   * @return DynamoDB key for item matching {@code path}
   */
  static PrimaryKey pathToKey(Path path) {
    path = removeSchemeAndAuthority(path);
    Preconditions.checkArgument(!path.isRoot(),
        "Root path is not mapped to any PrimaryKey");
    return new PrimaryKey(PARENT, path.getParent().toUri().getPath(),
        CHILD, path.getName());
  }

  /**
   * Converts a collection of {@link Path} to a collection of DynamoDB keys.
   *
   * @see #pathToKey(Path)
   */
  static PrimaryKey[] pathToKey(Collection<Path> paths) {
    Preconditions.checkNotNull(paths);
    final PrimaryKey[] keys = new PrimaryKey[paths.size()];
    int i = 0;
    for (Path p : paths) {
      keys[i++] = pathToKey(p);
    }
    return keys;
  }

  private static Path removeSchemeAndAuthority(Path path) {
    Preconditions.checkNotNull(path);
    return Path.getPathWithoutSchemeAndAuthority(path);
  }

  /**
   * There is no need to instantiate this class.
   */
  private PathMetadataDynamoDBTranslation() {
  }

}
