package org.apache.hadoop.mapreduce.v2.hs.webapp;

import java.util.Locale;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
import org.apache.hadoop.mapreduce.v2.app.job.Task;
import org.apache.hadoop.mapreduce.v2.app.webapp.App;
import org.apache.hadoop.mapreduce.v2.app.webapp.AppController;
import org.apache.hadoop.mapreduce.v2.app.webapp.CountersPage;
import org.apache.hadoop.mapreduce.v2.app.webapp.JobPage;
import org.apache.hadoop.mapreduce.v2.app.webapp.TasksPage;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.yarn.webapp.Controller;
import org.apache.hadoop.mapreduce.v2.util.MRApps;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import com.google.inject.Inject;

public class HsController extends AppController implements HSParams {
  
  @Inject HsController(App app, Configuration conf, RequestContext ctx) {
    super(app, conf, ctx, "History");
  }

  @Override
  public void index() {
    // TODO Auto-generated method stub
    setTitle("JobHistory");
  }

  public void job() {
    super.job();
  }

  public void jobCounters() {
    super.jobCounters();
  }

  public void tasks() {
    super.tasks();
  }
  
  public void task() {
    super.task();
  }

}
