/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Shell;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

public interface GetSpaceUsed {
  long getUsed() throws IOException;

  /**
   * The builder class
   */
  final class Builder {
    static final Logger LOG = LoggerFactory.getLogger(Builder.class);

    static final String CLASSNAME_KEY = "fs.getspaceused.classname";

    private Configuration conf;
    private Class<? extends GetSpaceUsed> klass = null;
    private File path = null;
    private Long interval = null;
    private Long initialUsed = null;

    public Configuration getConf() {
      return conf;
    }

    public Builder setConf(Configuration conf) {
      this.conf = conf;
      return this;
    }

    public long getInterval() {
      if (interval != null) {
        return interval;
      }
      long result = CommonConfigurationKeys.FS_DU_INTERVAL_DEFAULT;
      if (conf == null) {
        return result;
      }
      return conf.getLong(CommonConfigurationKeys.FS_DU_INTERVAL_KEY, result);
    }

    public Builder setInterval(long interval) {
      this.interval = interval;
      return this;
    }

    public Class<? extends GetSpaceUsed> getKlass() {
      if (klass != null) {
        return klass;
      }
      Class<? extends GetSpaceUsed> result = null;
      if (Shell.WINDOWS) {
        result = WindowsGetSpaceUsed.class;
      } else {
        result = DU.class;
      }
      if (conf == null) {
        return result;
      }
      return conf.getClass(CLASSNAME_KEY, result, GetSpaceUsed.class);
    }

    public Builder setKlass(Class<? extends GetSpaceUsed> klass) {
      this.klass = klass;
      return this;
    }

    public File getPath() {
      return path;
    }

    public Builder setPath(File path) {
      this.path = path;
      return this;
    }

    public long getInitialUsed() {
      if (initialUsed == null) {
        return -1;
      }
      return initialUsed;
    }

    public Builder setInitialUsed(long initialUsed) {
      this.initialUsed = initialUsed;
      return this;
    }

    public GetSpaceUsed build() throws IOException {
      GetSpaceUsed getSpaceUsed = null;
      try {
        Constructor<? extends GetSpaceUsed> cons =
            getKlass().getConstructor(Builder.class);
        getSpaceUsed = cons.newInstance(this);
      } catch (InstantiationException e) {
        LOG.warn("Error trying to create an instance of " + getKlass(), e);
      } catch (IllegalAccessException e) {
        LOG.warn("Error trying to create " + getKlass(), e);
      } catch (InvocationTargetException e) {
        LOG.warn("Error trying to create " + getKlass(), e);
      } catch (NoSuchMethodException e) {
        LOG.warn("Doesn't look like the class " + getKlass() +
            " have the needed constructor", e);
      }
      // If there were any exceptions then du will be null.
      // Construct our best guess fallback.
      if (getSpaceUsed == null) {
        if (Shell.WINDOWS) {
          getSpaceUsed = new WindowsGetSpaceUsed(this);
        } else {
          getSpaceUsed = new DU(this);
        }
      }
      // Call init after classes constructors have finished.
      if (getSpaceUsed instanceof CachingGetSpaceUsed) {
        ((CachingGetSpaceUsed) getSpaceUsed).init();
      }
      return getSpaceUsed;
    }

  }
}
