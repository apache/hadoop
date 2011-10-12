package org.apache.hadoop.mapreduce.v2.app.job.impl;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapred.TaskUmbilicalProtocol;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.split.JobSplit.TaskSplitMetaInfo;
import org.apache.hadoop.mapreduce.v2.api.records.JobId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskId;
import org.apache.hadoop.mapreduce.v2.api.records.TaskState;
import org.apache.hadoop.mapreduce.v2.api.records.TaskType;
import org.apache.hadoop.mapreduce.v2.app.TaskAttemptListener;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEvent;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskEventType;
import org.apache.hadoop.mapreduce.v2.app.job.event.TaskTAttemptEvent;
import org.apache.hadoop.mapreduce.v2.app.metrics.MRAppMetrics;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.Clock;
import org.apache.hadoop.yarn.SystemClock;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.resourcetracker.InlineDispatcher;
import org.apache.hadoop.yarn.util.Records;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestTaskImpl {

  private static final Log LOG = LogFactory.getLog(TestTaskImpl.class);    
  
  private Configuration conf;
  private TaskAttemptListener taskAttemptListener;
  private OutputCommitter committer;
  private Token<JobTokenIdentifier> jobToken;
  private JobId jobId;
  private Path remoteJobConfFile;
  private Collection<Token<? extends TokenIdentifier>> fsTokens;
  private Clock clock;
  private Set<TaskId> completedTasksFromPreviousRun;
  private MRAppMetrics metrics;
  private TaskImpl mockTask;
  private ApplicationId appId;
  private TaskSplitMetaInfo taskSplitMetaInfo;  
  private String[] dataLocations = new String[0]; 
  private final TaskType taskType = TaskType.MAP;
  
  private int startCount = 0;
  private int taskCounter = 0;
  private final int partition = 1;
  
  private InlineDispatcher dispatcher;   
  private List<MockTaskAttemptImpl> taskAttempts;
  
  private class MockTaskImpl extends TaskImpl {
        
    private int taskAttemptCounter = 0;

    @SuppressWarnings("rawtypes")
    public MockTaskImpl(JobId jobId, int partition,
        EventHandler eventHandler, Path remoteJobConfFile, Configuration conf,
        TaskAttemptListener taskAttemptListener, OutputCommitter committer,
        Token<JobTokenIdentifier> jobToken,
        Collection<Token<? extends TokenIdentifier>> fsTokens, Clock clock,
        Set<TaskId> completedTasksFromPreviousRun, int startCount,
        MRAppMetrics metrics) {
      super(jobId, taskType , partition, eventHandler,
          remoteJobConfFile, conf, taskAttemptListener, committer, 
          jobToken, fsTokens, clock, 
          completedTasksFromPreviousRun, startCount, metrics);
    }

    @Override
    public TaskType getType() {
      return taskType;
    }

    @Override
    protected TaskAttemptImpl createAttempt() {
      MockTaskAttemptImpl attempt = new MockTaskAttemptImpl(getID(), ++taskAttemptCounter, 
          eventHandler, taskAttemptListener, remoteJobConfFile, partition,
          conf, committer, jobToken, fsTokens, clock);
      taskAttempts.add(attempt);
      return attempt;
    }

    @Override
    protected int getMaxAttempts() {
      return 100;
    }    
    
  }
  
  private class MockTaskAttemptImpl extends TaskAttemptImpl {

    private float progress = 0;
    private TaskAttemptState state = TaskAttemptState.NEW;
    private TaskAttemptId attemptId;

    @SuppressWarnings("rawtypes")
    public MockTaskAttemptImpl(TaskId taskId, int id, EventHandler eventHandler,
        TaskAttemptListener taskAttemptListener, Path jobFile, int partition,
        Configuration conf, OutputCommitter committer,
        Token<JobTokenIdentifier> jobToken,
        Collection<Token<? extends TokenIdentifier>> fsTokens, Clock clock) {
      super(taskId, id, eventHandler, taskAttemptListener, jobFile, partition, conf,
          dataLocations, committer, jobToken, fsTokens, clock);
      attemptId = Records.newRecord(TaskAttemptId.class);
      attemptId.setId(id);
      attemptId.setTaskId(taskId);
    }

    public TaskAttemptId getAttemptId() {
      return attemptId;
    }
    
    @Override
    protected Task createRemoteTask() {
      return new MockTask();
    }    
    
    public float getProgress() {
      return progress ;
    }
    
    public void setProgress(float progress) {
      this.progress = progress;
    }
    
    public void setState(TaskAttemptState state) {
      this.state = state;
    }
    
    public TaskAttemptState getState() {
      return state;
    }
    
  }
  
  private class MockTask extends Task {

    @Override
    @SuppressWarnings("deprecation") 
    public void run(JobConf job, TaskUmbilicalProtocol umbilical)
        throws IOException, ClassNotFoundException, InterruptedException {
      return;
    }

    @Override
    public boolean isMapTask() {
      return true;
    }    
    
  }
  
  @Before 
  @SuppressWarnings("unchecked")
  public void setup() {
     dispatcher = new InlineDispatcher();
    
    ++startCount;
    
    conf = new Configuration();
    taskAttemptListener = mock(TaskAttemptListener.class);
    committer = mock(OutputCommitter.class);
    jobToken = (Token<JobTokenIdentifier>) mock(Token.class);
    remoteJobConfFile = mock(Path.class);
    fsTokens = null;
    clock = new SystemClock();
    metrics = mock(MRAppMetrics.class);  
    dataLocations = new String[1];
    
    appId = Records.newRecord(ApplicationId.class);
    appId.setClusterTimestamp(System.currentTimeMillis());
    appId.setId(1);

    jobId = Records.newRecord(JobId.class);
    jobId.setId(1);
    jobId.setAppId(appId);

    taskSplitMetaInfo = mock(TaskSplitMetaInfo.class);
    when(taskSplitMetaInfo.getLocations()).thenReturn(dataLocations); 
    
    taskAttempts = new ArrayList<MockTaskAttemptImpl>();
    
    mockTask = new MockTaskImpl(jobId, partition, dispatcher.getEventHandler(),
        remoteJobConfFile, conf, taskAttemptListener, committer, jobToken,
        fsTokens, clock, 
        completedTasksFromPreviousRun, startCount,
        metrics);        
    
  }

  @After 
  public void teardown() {
    taskAttempts.clear();
  }
  
  private TaskId getNewTaskID() {
    TaskId taskId = Records.newRecord(TaskId.class);
    taskId.setId(++taskCounter);
    taskId.setJobId(jobId);
    taskId.setTaskType(mockTask.getType());    
    return taskId;
  }
  
  private void scheduleTaskAttempt(TaskId taskId) {
    mockTask.handle(new TaskEvent(taskId, 
        TaskEventType.T_SCHEDULE));
    assertTaskScheduledState();
  }
  
  private void killTask(TaskId taskId) {
    mockTask.handle(new TaskEvent(taskId, 
        TaskEventType.T_KILL));
    assertTaskKillWaitState();
  }
  
  private void killScheduledTaskAttempt(TaskAttemptId attemptId) {
    mockTask.handle(new TaskTAttemptEvent(attemptId, 
        TaskEventType.T_ATTEMPT_KILLED));
    assertTaskScheduledState();
  }

  private void launchTaskAttempt(TaskAttemptId attemptId) {
    mockTask.handle(new TaskTAttemptEvent(attemptId, 
        TaskEventType.T_ATTEMPT_LAUNCHED));
    assertTaskRunningState();    
  }
  
  private MockTaskAttemptImpl getLastAttempt() {
    return taskAttempts.get(taskAttempts.size()-1);
  }
  
  private void updateLastAttemptProgress(float p) {    
    getLastAttempt().setProgress(p);
  }

  private void updateLastAttemptState(TaskAttemptState s) {
    getLastAttempt().setState(s);
  }
  
  private void killRunningTaskAttempt(TaskAttemptId attemptId) {
    mockTask.handle(new TaskTAttemptEvent(attemptId, 
        TaskEventType.T_ATTEMPT_KILLED));
    assertTaskRunningState();  
  }
  
  /**
   * {@link TaskState#NEW}
   */
  private void assertTaskNewState() {
    assertEquals(mockTask.getState(), TaskState.NEW);
  }
  
  /**
   * {@link TaskState#SCHEDULED}
   */
  private void assertTaskScheduledState() {
    assertEquals(mockTask.getState(), TaskState.SCHEDULED);
  }

  /**
   * {@link TaskState#RUNNING}
   */
  private void assertTaskRunningState() {
    assertEquals(mockTask.getState(), TaskState.RUNNING);        
  }
    
  /**
   * {@link TaskState#KILL_WAIT}
   */
  private void assertTaskKillWaitState() {
    assertEquals(mockTask.getState(), TaskState.KILL_WAIT);
  }
  
  @Test
  public void testInit() {
    LOG.info("--- START: testInit ---");
    assertTaskNewState();
    assert(taskAttempts.size() == 0);
  }

  @Test
  /**
   * {@link TaskState#NEW}->{@link TaskState#SCHEDULED}
   */
  public void testScheduleTask() {
    LOG.info("--- START: testScheduleTask ---");
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
  }
  
  @Test 
  /**
   * {@link TaskState#SCHEDULED}->{@link TaskState#KILL_WAIT}
   */
  public void testKillScheduledTask() {
    LOG.info("--- START: testKillScheduledTask ---");
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    killTask(taskId);
  }
  
  @Test 
  /**
   * Kill attempt
   * {@link TaskState#SCHEDULED}->{@link TaskState#SCHEDULED}
   */
  public void testKillScheduledTaskAttempt() {
    LOG.info("--- START: testKillScheduledTaskAttempt ---");
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    killScheduledTaskAttempt(getLastAttempt().getAttemptId());
  }
  
  @Test 
  /**
   * Launch attempt
   * {@link TaskState#SCHEDULED}->{@link TaskState#RUNNING}
   */
  public void testLaunchTaskAttempt() {
    LOG.info("--- START: testLaunchTaskAttempt ---");
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(getLastAttempt().getAttemptId());
  }

  @Test
  /**
   * Kill running attempt
   * {@link TaskState#RUNNING}->{@link TaskState#RUNNING} 
   */
  public void testKillRunningTaskAttempt() {
    LOG.info("--- START: testKillRunningTaskAttempt ---");
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    launchTaskAttempt(getLastAttempt().getAttemptId());
    killRunningTaskAttempt(getLastAttempt().getAttemptId());    
  }

  @Test 
  public void testTaskProgress() {
    LOG.info("--- START: testTaskProgress ---");
        
    // launch task
    TaskId taskId = getNewTaskID();
    scheduleTaskAttempt(taskId);
    float progress = 0f;
    assert(mockTask.getProgress() == progress);
    launchTaskAttempt(getLastAttempt().getAttemptId());    
    
    // update attempt1 
    progress = 50f;
    updateLastAttemptProgress(progress);
    assert(mockTask.getProgress() == progress);
    progress = 100f;
    updateLastAttemptProgress(progress);
    assert(mockTask.getProgress() == progress);
    
    progress = 0f;
    // mark first attempt as killed
    updateLastAttemptState(TaskAttemptState.KILLED);
    assert(mockTask.getProgress() == progress);

    // kill first attempt 
    // should trigger a new attempt
    // as no successful attempts 
    killRunningTaskAttempt(getLastAttempt().getAttemptId());
    assert(taskAttempts.size() == 2);
    
    assert(mockTask.getProgress() == 0f);
    launchTaskAttempt(getLastAttempt().getAttemptId());
    progress = 50f;
    updateLastAttemptProgress(progress);
    assert(mockTask.getProgress() == progress);
        
  }

}
