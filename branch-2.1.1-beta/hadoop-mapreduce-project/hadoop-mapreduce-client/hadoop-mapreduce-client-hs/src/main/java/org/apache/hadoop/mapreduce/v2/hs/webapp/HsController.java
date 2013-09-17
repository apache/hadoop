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

package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.AppController;
import org.apache.hadoop.yarn.webapp.View;
import org.apache.hadoop.yarn.webapp.log.AggregatedLogsPage;

import com.google.inject.Inject;

/**
 * This class renders the various pages that the History Server WebApp supports
 */
public class HsController extends AppController {
  
  
  @Inject HsController(App app, Configuration conf, RequestContext ctx) {
    super(app, conf, ctx, "History");
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#index()
   */
  @Override
  public void index() {
    setTitle("JobHistory");
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#jobPage()
   */
  @Override
  protected Class<? extends View> jobPage() {
    return HsJobPage.class;
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#countersPage()
   */
  @Override
  public Class<? extends View> countersPage() {
    return HsCountersPage.class;
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#tasksPage()
   */
  @Override
  protected Class<? extends View> tasksPage() {
    return HsTasksPage.class;
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#taskPage()
   */
  @Override
  protected Class<? extends View> taskPage() {
    return HsTaskPage.class;
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#attemptsPage()
   */
  @Override
  protected Class<? extends View> attemptsPage() {
    return HsAttemptsPage.class;
  }
  
  // Need all of these methods here also as Guice doesn't look into parent
  // classes.
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#job()
   */
  @Override
  public void job() {
    super.job();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#jobCounters()
   */
  @Override
  public void jobCounters() {
    super.jobCounters();
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#taskCounters()
   */
  @Override
  public void taskCounters() {
    super.taskCounters();
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#tasks()
   */
  @Override
  public void tasks() {
    super.tasks();
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#task()
   */
  @Override
  public void task() {
    super.task();
  }

  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#attempts()
   */
  @Override
  public void attempts() {
    super.attempts();
  }
  
  /**
   * @return the page that will be used to render the /conf page
   */
  @Override
  protected Class<? extends View> confPage() {
    return HsConfPage.class;
  }

  /**
   * @return the page about the current server.
   */
  protected Class<? extends View> aboutPage() {
    return HsAboutPage.class;
  }
  
  /**
   * Render a page about the current server.
   */
  public void about() {
    render(aboutPage());
  }
  
  /**
   * Render the logs page.
   */
  public void logs() {
    render(HsLogsPage.class);
  }

  /**
   * Render the nm logs page.
   */
  public void nmlogs() {
    render(AggregatedLogsPage.class);
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#singleCounterPage()
   */
  @Override
  protected Class<? extends View> singleCounterPage() {
    return HsSingleCounterPage.class;
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#singleJobCounter()
   */
  @Override
  public void singleJobCounter() throws IOException{
    super.singleJobCounter();
  }
  
  /*
   * (non-Javadoc)
   * @see org.apache.hadoop.mapreduce.v2.app.webapp.AppController#singleTaskCounter()
   */
  @Override
  public void singleTaskCounter() throws IOException{
    super.singleTaskCounter();
  }
}
