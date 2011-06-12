package org.apache.hadoop.mapreduce.v2.hs.webapp;

import org.apache.hadoop.mapreduce.v2.app.AppContext;
import org.apache.hadoop.mapreduce.v2.hs.HistoryClientService;
import org.apache.hadoop.mapreduce.v2.hs.HistoryContext;
import org.apache.hadoop.yarn.webapp.WebApp;
import static org.apache.hadoop.yarn.util.StringHelper.*;
public class HSWebApp extends WebApp implements HSParams {

  private HistoryContext history;

  public HSWebApp(HistoryContext history) {
    this.history = history;
  }

  @Override
  public void setup() {
    bind(AppContext.class).toInstance(history);
    route("/", HsController.class);
    route("/app", HsController.class);
    route(pajoin("/job", JOB_ID), HsController.class, "job");
    route(pajoin("/jobcounters", JOB_ID), HsController.class, "jobCounters");
    route(pajoin("/tasks", JOB_ID, TASK_TYPE), HsController.class, "tasks");
    route(pajoin("/task", TASK_ID), HsController.class, "task");
  }
}

