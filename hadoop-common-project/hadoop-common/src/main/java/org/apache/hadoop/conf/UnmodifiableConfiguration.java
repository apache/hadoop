/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.conf;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import com.google.common.collect.Iterators;

import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.fs.Path;

/**
 * An unmodifiable view of a Configuration.
 */
@Unstable
class UnmodifiableConfiguration extends Configuration {

  private final Configuration other;

  UnmodifiableConfiguration(Configuration other) {
    super(false);
    this.other = other;
  }

  @Override
  public Iterator<Map.Entry<String, String>> iterator() {
    return Iterators.unmodifiableIterator(other.iterator());
  }

  @Override
  public String get(String name) {
    return this.other.get(name);
  }

  @Override
  public boolean onlyKeyExists(String name) {
    return this.other.onlyKeyExists(name);
  }

  @Override
  public String getTrimmed(String name) {
    return this.other.getTrimmed(name);
  }

  @Override
  public String getTrimmed(String name, String defaultValue) {
    return this.other.getTrimmed(name, defaultValue);
  }

  @Override
  public String getRaw(String name) {
    return this.other.getRaw(name);
  }

  @Override
  public String get(String name, String defaultValue) {
    return this.other.get(name, defaultValue);
  }

  @Override
  public int getInt(String name, int defaultValue) {
    return this.other.getInt(name, defaultValue);
  }

  @Override
  public int[] getInts(String name) {
    return this.other.getInts(name);
  }

  @Override
  public long getLong(String name, long defaultValue) {
    return this.other.getLong(name, defaultValue);
  }

  @Override
  public long getLongBytes(String name, long defaultValue) {
    return this.other.getLongBytes(name, defaultValue);
  }

  @Override
  public float getFloat(String name, float defaultValue) {
    return this.other.getFloat(name, defaultValue);
  }

  @Override
  public double getDouble(String name, double defaultValue) {
    return this.other.getDouble(name, defaultValue);
  }

  @Override
  public boolean getBoolean(String name, boolean defaultValue) {
    return this.other.getBoolean(name, defaultValue);
  }

  @Override
  public <T extends Enum<T>> T getEnum(String name, T defaultValue) {
    return this.other.getEnum(name, defaultValue);
  }

  @Override
  public long getTimeDuration(String name, long defaultValue, TimeUnit unit) {
    return this.other.getTimeDuration(name, defaultValue, unit);
  }

  @Override
  public long getTimeDuration(String name, String defaultValue, TimeUnit unit) {
    return this.other.getTimeDuration(name, defaultValue, unit);
  }

  @Override
  public long getTimeDurationHelper(String name, String vStr, TimeUnit unit) {
    return this.other.getTimeDurationHelper(name, vStr, unit);
  }

  @Override
  public long[] getTimeDurations(String name, TimeUnit unit) {
    return this.other.getTimeDurations(name, unit);
  }

  @Override
  public double getStorageSize(String name, String defaultValue,
                               StorageUnit targetUnit) {
    return this.other.getStorageSize(name, defaultValue, targetUnit);
  }

  @Override
  public double getStorageSize(String name, double defaultValue,
                               StorageUnit targetUnit) {
    return this.other.getStorageSize(name, defaultValue, targetUnit);
  }

  @Override
  public Pattern getPattern(String name, Pattern defaultValue) {
    return this.other.getPattern(name, defaultValue);
  }

  @Override
  public synchronized String[] getPropertySources(String name) {
    return this.other.getPropertySources(name);
  }

  @Override
  public IntegerRanges getRange(String name, String defaultValue) {
    return this.other.getRange(name, defaultValue);
  }

  @Override
  public Collection<String> getStringCollection(String name) {
    return this.other.getStringCollection(name);
  }

  @Override
  public String[] getStrings(String name) {
    return this.other.getStrings(name);
  }

  @Override
  public String[] getStrings(String name, String... defaultValue) {
    return this.other.getStrings(name, defaultValue);
  }

  @Override
  public Collection<String> getTrimmedStringCollection(String name) {
    return this.other.getTrimmedStringCollection(name);
  }

  @Override
  public String[] getTrimmedStrings(String name) {
    return this.other.getTrimmedStrings(name);
  }

  @Override
  public String[] getTrimmedStrings(String name, String... defaultValue) {
    return this.other.getTrimmedStrings(name, defaultValue);
  }

  @Override
  public char[] getPassword(String name) throws IOException {
    return this.other.getPassword(name);
  }

  @Override
  public char[] getPasswordFromCredentialProviders(String name)
      throws IOException {
    return this.other.getPasswordFromCredentialProviders(name);
  }

  @Override
  public InetSocketAddress getSocketAddr(String hostProperty,
      String addressProperty, String defaultAddressValue, int defaultPort) {
    return this.other.getSocketAddr(hostProperty, addressProperty,
            defaultAddressValue, defaultPort);
  }

  @Override
  public InetSocketAddress getSocketAddr(String name, String defaultAddress,
      int defaultPort) {
    return this.other.getSocketAddr(name, defaultAddress, defaultPort);
  }

  @Override
  public Class<?> getClassByName(String name) throws ClassNotFoundException {
    return this.other.getClassByName(name);
  }

  @Override
  public Class<?> getClassByNameOrNull(String name) {
    return this.other.getClassByNameOrNull(name);
  }

  @Override
  public Class<?>[] getClasses(String name, Class<?>... defaultValue) {
    return this.other.getClasses(name, defaultValue);
  }

  @Override
  public Class<?> getClass(String name, Class<?> defaultValue) {
    return this.other.getClass(name, defaultValue);
  }

  @Override
  public <U> Class<? extends U> getClass(String name,
                                         Class<? extends U> defaultValue,
                                         Class<U> xface) {
    return this.other.getClass(name, defaultValue, xface);
  }

  @Override
  public <U> List<U> getInstances(String name, Class<U> xface) {
    return this.other.getInstances(name, xface);
  }

  @Override
  public Path getLocalPath(String dirsProp, String path) throws IOException {
    return this.other.getLocalPath(dirsProp, path);
  }

  @Override
  public File getFile(String dirsProp, String path) throws IOException {
    return this.other.getFile(dirsProp, path);
  }

  @Override
  public URL getResource(String name) {
    return this.other.getResource(name);
  }

  @Override
  public InputStream getConfResourceAsInputStream(String name) {
    return this.other.getConfResourceAsInputStream(name);
  }

  @Override
  public Reader getConfResourceAsReader(String name) {
    return this.other.getConfResourceAsReader(name);
  }

  @Override
  public Set<String> getFinalParameters() {
    return this.other.getFinalParameters();
  }

  @Override
  public int size() {
    return this.other.size();
  }

  @Override
  public Map<String, String> getPropsWithPrefix(String confPrefix) {
    return this.other.getPropsWithPrefix(confPrefix);
  }

  @Override
  public ClassLoader getClassLoader() {
    return this.other.getClassLoader();
  }

  @Override
  public String toString() {
    return this.other.toString();
  }

  @Override
  public Map<String, String> getValByRegex(String regex) {
    return this.other.getValByRegex(regex);
  }

  @Override
  public Properties getAllPropertiesByTag(String tag) {
    return this.other.getAllPropertiesByTag(tag);
  }

  @Override
  public Properties getAllPropertiesByTags(List<String> tagList) {
    return this.other.getAllPropertiesByTags(tagList);
  }

  @Override
  public boolean isPropertyTag(String tagStr) {
    return this.other.isPropertyTag(tagStr);
  }

  // all the mutable methods below should throw an error
  @Override
  public void readFields(DataInput in) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDeprecatedProperties() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRestrictSystemProperties(boolean val) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(String name, boolean restrictedParser) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(URL url) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(URL url, boolean restrictedParser) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(Path file) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(Path file, boolean restrictedParser) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(InputStream in) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(InputStream in, boolean restrictedParser) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(InputStream in, String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(InputStream in, String name,
                          boolean restrictedParser) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void addResource(Configuration conf) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void reloadConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setAllowNullValueProperties(boolean val) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setRestrictSystemProps(boolean val) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void set(String name, String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void set(String name, String value, String source) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void unset(String name) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void setIfUnset(String name, String value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setInt(String name, int value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setLong(String name, long value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setFloat(String name, float value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setDouble(String name, double value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBoolean(String name, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setBooleanIfUnset(String name, boolean value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T extends Enum<T>> void setEnum(String name, T value) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setTimeDuration(String name, long value, TimeUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStorageSize(String name, double value, StorageUnit unit) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setPattern(String name, Pattern pattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setStrings(String name, String... values) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setSocketAddr(String name, InetSocketAddress addr) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InetSocketAddress updateConnectAddr(
          String hostProperty,
          String addressProperty,
          String defaultAddressValue,
          InetSocketAddress addr) {
    throw new UnsupportedOperationException();
  }

  @Override
  public InetSocketAddress updateConnectAddr(String name,
                                             InetSocketAddress addr) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClass(String name, Class<?> theClass, Class<?> xface) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeXml(OutputStream out) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeXml(Writer out) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void writeXml(String propertyName, Writer out)
          throws IllegalArgumentException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setClassLoader(ClassLoader classLoader) {
    throw new UnsupportedOperationException();
  }

  @Override
  public synchronized void setQuietMode(boolean quietmode) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    throw new UnsupportedOperationException();
  }
}
