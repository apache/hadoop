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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.resolver;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An immutable copy of a {@link Configuration}, taken once, with a prefix
 * index for per-queue and per-label lookups.
 * <p>
 * {@link #get(String)} and {@link #getPropertiesWithPrefix(String)} return
 * the value {@code conf.get(key)} returned when the snapshot was taken, so
 * deprecation handling and variable substitution are applied. The raw
 * accessors return the unexpanded text stored in the configuration, which is
 * what the prefix lookups of the Capacity Scheduler have always used.
 * <p>
 * The prefix index is a trie with one node per key part delimited by ".".
 * A key is stored in the node of its last part, so a prefix query walks the
 * prefix parts and collects the whole subtree below the last one. Prefixes
 * therefore match whole key parts only: {@code root.a} matches
 * {@code root.a.capacity} but not {@code root.ab.capacity}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class ConfigSnapshot {
  private static final Logger LOG =
      LoggerFactory.getLogger(ConfigSnapshot.class);

  private static final String DELIMITER = "\\.";
  private static final String DOT = ".";

  /** Key to the value {@code conf.get(key)} returned. */
  private final Map<String, String> values;
  /**
   * Key to the raw text, only for keys whose raw text differs from the
   * value, which keeps the common case free of a second copy.
   */
  private final Map<String, String> rawValues;
  /** Keys whose {@code conf.get(key)} threw, rethrown on access. */
  private final Map<String, RuntimeException> failures;
  private final Set<String> keys;
  private final PrefixNode root = new PrefixNode();

  private ConfigSnapshot(Map<String, String> values,
      Map<String, String> rawValues, Map<String, RuntimeException> failures,
      Set<String> keys) {
    this.values = values;
    this.rawValues = rawValues;
    this.failures = failures;
    this.keys = Collections.unmodifiableSet(keys);
    for (String key : keys) {
      index(key);
    }
  }

  /**
   * Takes a snapshot of every property of a configuration.
   * @param conf the configuration to copy
   * @return the snapshot
   */
  public static ConfigSnapshot of(Configuration conf) {
    Map<String, String> raw = new HashMap<>();
    // The iterator returns a copy of the raw properties, so the conf.get
    // calls below, which may update deprecated keys, do not disturb it.
    Iterator<Map.Entry<String, String>> it = conf.iterator();
    while (it.hasNext()) {
      Map.Entry<String, String> entry = it.next();
      raw.put(entry.getKey(), entry.getValue());
    }

    Map<String, String> values = new HashMap<>(raw.size() * 4 / 3 + 1);
    Map<String, String> rawValues = new HashMap<>();
    Map<String, RuntimeException> failures = new HashMap<>();
    for (Map.Entry<String, String> entry : raw.entrySet()) {
      String key = entry.getKey();
      String rawValue = entry.getValue();
      // conf.get only differs from the raw text when the value references a
      // variable or the key is deprecated; skipping the call keeps the
      // snapshot cheap for the common case.
      if (rawValue.indexOf('$') < 0 && !Configuration.isDeprecated(key)) {
        values.put(key, rawValue);
        continue;
      }
      try {
        String value = conf.get(key);
        if (value != null) {
          values.put(key, value);
        }
        if (!rawValue.equals(value)) {
          rawValues.put(key, rawValue);
        }
      } catch (RuntimeException e) {
        failures.put(key, e);
        rawValues.put(key, rawValue);
      }
    }
    return new ConfigSnapshot(values, rawValues, failures, raw.keySet());
  }

  /**
   * Takes a snapshot of plain properties, using every value as it is, without
   * deprecation handling or variable substitution.
   * @param properties the properties to copy
   * @return the snapshot
   */
  public static ConfigSnapshot of(Map<String, String> properties) {
    return new ConfigSnapshot(new HashMap<>(properties),
        Collections.<String, String>emptyMap(),
        Collections.<String, RuntimeException>emptyMap(),
        new HashSet<>(properties.keySet()));
  }

  /**
   * Returns the value of a property.
   * @param key the full property key
   * @return the value {@code conf.get(key)} returned, or {@code null} when
   *         the property is absent
   * @throws RuntimeException the exception {@code conf.get(key)} threw, for
   *         example on a too deep variable substitution
   */
  public String get(String key) {
    RuntimeException failure = failures.get(key);
    if (failure != null) {
      throw failure;
    }
    return values.get(key);
  }

  /**
   * Returns the raw text of a property, before variable substitution.
   * @param key the full property key
   * @return the raw text, or {@code null} when the property is absent
   */
  public String getRaw(String key) {
    String raw = rawValues.get(key);
    return raw != null ? raw : values.get(key);
  }

  /**
   * Returns every key of the snapshot.
   * @return an unmodifiable set of the keys
   */
  public Set<String> keys() {
    return keys;
  }

  /**
   * Filters the properties by a prefix of whole key parts. The returned keys
   * are trimmed by the prefix, a trailing dot of the prefix is disregarded,
   * and a key equal to the prefix is returned as the empty string.
   * @param prefix the key prefix
   * @return a new map of the trimmed keys to their values
   */
  public Map<String, String> getPropertiesWithPrefix(String prefix) {
    return getPropertiesWithPrefix(prefix, false);
  }

  /**
   * Filters the properties by a prefix of whole key parts.
   * @param prefix the key prefix
   * @param fullyQualifiedKey whether to keep the keys as they are instead of
   *                          trimming them by the prefix
   * @return a new map of the keys to their values
   */
  public Map<String, String> getPropertiesWithPrefix(String prefix,
      boolean fullyQualifiedKey) {
    return collect(prefix, fullyQualifiedKey, false);
  }

  /**
   * Same as {@link #getPropertiesWithPrefix(String, boolean)}, but returns
   * the raw text of the values, before variable substitution.
   * @param prefix the key prefix
   * @param fullyQualifiedKey whether to keep the keys as they are instead of
   *                          trimming them by the prefix
   * @return a new map of the keys to their raw values
   */
  public Map<String, String> getRawPropertiesWithPrefix(String prefix,
      boolean fullyQualifiedKey) {
    return collect(prefix, fullyQualifiedKey, true);
  }

  private Map<String, String> collect(String prefix,
      boolean fullyQualifiedKey, boolean raw) {
    Map<String, String> properties = new HashMap<>();
    PrefixNode node = root;
    for (String part : prefix.split(DELIMITER)) {
      node = node.getChild(part);
      if (node == null) {
        return properties;
      }
    }

    String trimPrefix;
    if (fullyQualifiedKey) {
      trimPrefix = "";
    } else {
      // Queue prefixes end with a dot, which is not part of the trimmed key
      trimPrefix = prefix.endsWith(DOT) ?
          prefix.substring(0, prefix.length() - 1) : prefix;
    }
    collectRecursively(node, properties, trimPrefix, raw);
    return properties;
  }

  private void collectRecursively(PrefixNode node,
      Map<String, String> properties, String trimPrefix, boolean raw) {
    if (node.keys != null) {
      for (String key : node.keys) {
        String value = raw ? getRaw(key) : get(key);
        if (value == null) {
          continue;
        }
        String trimmedKey = key;
        if (!trimPrefix.isEmpty()) {
          int trimLength = key.equals(trimPrefix) ?
              trimPrefix.length() : trimPrefix.length() + 1;
          trimmedKey = key.substring(trimLength);
        }
        properties.put(trimmedKey, value);
      }
    }
    if (node.children != null) {
      for (PrefixNode child : node.children.values()) {
        collectRecursively(child, properties, trimPrefix, raw);
      }
    }
  }

  private void index(String key) {
    String[] parts = key.split(DELIMITER);
    if (parts.length == 0) {
      LOG.warn("Empty configuration property, skipping...");
      return;
    }
    PrefixNode node = root;
    for (String part : parts) {
      node = node.getOrCreateChild(part);
    }
    node.addKey(key);
  }

  /**
   * A node of the prefix index, for example {@code yarn.scheduler} consists
   * of a "yarn" and a "scheduler" node. Both fields are allocated on first
   * use, since most nodes hold either keys or children but not both.
   */
  private static final class PrefixNode {
    private Map<String, PrefixNode> children;
    private List<String> keys;

    private PrefixNode getChild(String part) {
      return children == null ? null : children.get(part);
    }

    private PrefixNode getOrCreateChild(String part) {
      if (children == null) {
        children = new HashMap<>();
      }
      PrefixNode child = children.get(part);
      if (child == null) {
        child = new PrefixNode();
        children.put(part, child);
      }
      return child;
    }

    private void addKey(String key) {
      if (keys == null) {
        keys = new ArrayList<>(1);
      }
      keys.add(key);
    }
  }
}
