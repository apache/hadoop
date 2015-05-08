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

package org.apache.hadoop.mapreduce.v2.app.webapp;

import java.util.HashMap;
import java.util.Map;

import com.sun.jersey.api.json.JSONConfiguration;
import com.sun.jersey.api.json.JSONJAXBContext;
import com.google.inject.Singleton;

import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;
import javax.xml.bind.JAXBContext;

import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AMAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AMAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.AppInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ConfEntryInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.CounterGroupInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.CounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptState;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskAttemptCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.JobTaskCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.ReduceTaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskAttemptsInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskCounterGroupInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskCounterInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TaskInfo;
import org.apache.hadoop.mapreduce.v2.app.webapp.dao.TasksInfo;
import org.apache.hadoop.yarn.webapp.RemoteExceptionData;

@Singleton
@Provider
public class JAXBContextResolver implements ContextResolver<JAXBContext> {

  private final Map<Class, JAXBContext> typesContextMap;

  // you have to specify all the dao classes here
  private final Class[] cTypes = {AMAttemptInfo.class, AMAttemptsInfo.class,
    AppInfo.class, CounterInfo.class, JobTaskAttemptCounterInfo.class,
    JobTaskCounterInfo.class, TaskCounterGroupInfo.class, ConfInfo.class,
    JobCounterInfo.class, TaskCounterInfo.class, CounterGroupInfo.class,
    JobInfo.class, JobsInfo.class, ReduceTaskAttemptInfo.class,
    TaskAttemptInfo.class, TaskInfo.class, TasksInfo.class,
    TaskAttemptsInfo.class, ConfEntryInfo.class, RemoteExceptionData.class};

  // these dao classes need root unwrapping
  private final Class[] rootUnwrappedTypes = {JobTaskAttemptState.class};

  public JAXBContextResolver() throws Exception {
    JAXBContext context;
    JAXBContext unWrappedRootContext;

    this.typesContextMap = new HashMap<Class, JAXBContext>();
    context =
        new JSONJAXBContext(JSONConfiguration.natural().rootUnwrapping(false)
            .build(), cTypes);
    unWrappedRootContext =
        new JSONJAXBContext(JSONConfiguration.natural().rootUnwrapping(true)
            .build(), rootUnwrappedTypes);
    for (Class type : cTypes) {
      typesContextMap.put(type, context);
    }
    for (Class type : rootUnwrappedTypes) {
      typesContextMap.put(type, unWrappedRootContext);
    }
  }

  @Override
  public JAXBContext getContext(Class<?> objectType) {
    return typesContextMap.get(objectType);
  }
}
