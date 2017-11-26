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
package org.apache.hadoop.log.metrics;

import java.io.Serializable;

import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginBuilderFactory;

@Plugin(name = "HadoopEventCounter", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE, printObject = true)
public class Log4j2EventCounter extends AbstractAppender {

    public Log4j2EventCounter(String name, final Filter filter, final Layout<? extends Serializable> layout) {
        super(name, filter, layout);
    }

    @Override
    public void append(LogEvent logEvent) {
        switch(logEvent.getLevel().getStandardLevel()) {
            case FATAL:
                EventCount.FATAL.incr();
                break;
            case ERROR:
                EventCount.ERROR.incr();
                break;
            case WARN:
                EventCount.WARN.incr();
                break;
            case INFO:
                EventCount.INFO.incr();
                break;
        }
    }

    @PluginBuilderFactory
    public static <B extends Log4j2EventCounter.Builder<B>> B newBuilder() {
        return new Log4j2EventCounter.Builder<B>().asBuilder();
    }

    /**
     * Builds Log4j2EventCounter instances.
     * @param <B> The type to build
     */
    public static class Builder<B extends Builder<B>> extends AbstractAppender.Builder<B>
            implements org.apache.logging.log4j.core.util.Builder<Log4j2EventCounter> {

        @Override
        public Log4j2EventCounter build() {
            return new Log4j2EventCounter(getName(), getFilter(), getLayout());
        }
    }
}
