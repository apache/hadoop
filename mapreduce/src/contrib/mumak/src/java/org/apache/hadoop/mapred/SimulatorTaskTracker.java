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
package org.apache.hadoop.mapred;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.TaskStatus.Phase;
import org.apache.hadoop.mapred.TaskStatus.State;
import org.apache.hadoop.tools.rumen.TaskAttemptInfo;
import org.apache.hadoop.tools.rumen.ReduceTaskAttemptInfo;
// Explicitly use the new api, older o.a.h.mapred.TaskAttemptID is deprecated
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.conf.Configuration;

/**
 * This class simulates a {@link TaskTracker}. Its main purpose is to call heartbeat()
 * of the simulated Job Tracker with apropriately updated statuses of the 
 * tasks assigned to it.
 *
 * The events emitted and consumed are HeartbeatEvent and 
 * TaskAttemptCompletionEvent .
 *
 * Internal naming convention: accept() dispatches simulation events to 
 * process*Event() methods. heartbeat() dispactches task tracker actions to 
 * handle*Action() methods.
 */
public class SimulatorTaskTracker implements SimulatorEventListener {
  /** Default host name. */
  public static String DEFAULT_HOST_NAME = "unknown";
  /** Default task tracker name. */
  public static String DEFAULT_TRACKER_NAME = 
      "tracker_unknown:localhost/127.0.0.1:10000";
  /** Default number of map slots per task tracker. */
  public static final int DEFAULT_MAP_SLOTS = 2;
  /** Default number of reduce slots per task tracker. */
  public static final int DEFAULT_REDUCE_SLOTS = 2;
  /** Default range of heartbeat response perturbations + 1 in milliseconds. */
  public static final int DEFAULT_HEARTBEAT_FUZZ = 11;
  /** The name of the task tracker. */
  protected final String taskTrackerName;
  /** The name of the host the task tracker is running on. */
  protected final String hostName;
  /** The http port the simulated task tracker reports to the jobtracker. */  
  protected final int httpPort = 80;
  /** Number of map slots. */
  protected final int maxMapSlots;
  /** Number of reduce slots. */
  protected final int maxReduceSlots;
  /** The job tracker this task tracker is a slave of. */
  protected final InterTrackerProtocol jobTracker;

  /**
   * State of and bookkeeping information for all tasks assigned to the task 
   * tracker. Contains all the information about running or completed but
   * not yet reported tasks. We manage it in a mark & sweep garbage collector 
   * manner. We insert tasks on launch, mark them on completion, and remove
   * completed tasks at heartbeat() reports.
   * We use LinkedHashMap instead of HashMap so that the order of iteration
   * is deterministic.
   */
  protected Map<TaskAttemptID, SimulatorTaskInProgress> tasks = 
      new LinkedHashMap<TaskAttemptID, SimulatorTaskInProgress>();
  /** 
   * Number of map slots allocated to tasks in RUNNING state on this task 
   * tracker. Must be in sync with the tasks map above. 
   */
  private int usedMapSlots = 0;
  /**
   * Number of reduce slots allocated to tasks in RUNNING state on this task
   * tracker. Must be in sync with the tasks map above.
   */
  private int usedReduceSlots = 0;  
  /**
   * True if the jobTracker.heartbeat() call to be made is the first one.
   * We need this to mimick the InterTrackerProtocol properly.
   */
  private boolean firstHeartbeat = true;

  // last heartbeat response recieved
  private short heartbeatResponseId = -1;

  /**
   * Task attempt ids for which TaskAttemptCompletionEvent was created but the 
   * task attempt got killed.
   * We use LinkedHashSet to get deterministic iterators, should ever use one.
   */
  private Set<TaskAttemptID> orphanTaskCompletions = 
    new LinkedHashSet<TaskAttemptID>();

  /** The log object to send our messages to; only used for debugging. */
  private static final Log LOG = LogFactory.getLog(SimulatorTaskTracker.class);
  
  /** 
   * Number of milliseconds to perturb the requested heartbeat intervals with
   * to simulate network latency, etc.
   * If <= 1 then there is no pertrubation. This option is also useful for 
   * testing.
   * If > 1 then hearbeats are perturbed with a uniformly random integer in 
   * (-heartbeatIntervalFuzz,+heartbeatIntervalFuzz), not including 
   * the bounds.
   */
  private int heartbeatIntervalFuzz = -1;
  /** Used for randomly perturbing the heartbeat timings. */
  private Random random;
  
  /**
   * Constructs a task tracker. 
   *
   * @param jobTracker the SimulatorJobTracker we talk to
   * @param conf Configuration object. Parameters read are:
   * <dl>
   * <dt> mumak.tasktracker.tracker.name <dd> 
   *      the task tracker name to report, otherwise unused
   * <dt> mumak.tasktracker.host.name <dd> 
   *      the host name to report, otherwise unused
   * <dt> mapred.tasktracker.map.tasks.maximum <dd> 
   *      the number of map slots
   * <dt> mapred.tasktracker.reduce.tasks.maximum <dd> 
   *      the number of reduce slots
   * <dt> mumak.tasktracker.heartbeat.fuzz <dd>
   *      Perturbation for the heartbeats. 
   *      None if <= 1 else perturbations are uniformly randomly generated 
   *      in (-heartbeat.fuzz,+heartbeat.fuzz), not including the bounds.
   * </dl>
   */
  public SimulatorTaskTracker(InterTrackerProtocol jobTracker,
                              Configuration conf) {
    this.taskTrackerName = conf.get(
        "mumak.tasktracker.tracker.name", DEFAULT_TRACKER_NAME);

    LOG.debug("SimulatorTaskTracker constructor, taskTrackerName=" +
              taskTrackerName);

    this.jobTracker = jobTracker;    
    this.hostName = conf.get(
        "mumak.tasktracker.host.name", DEFAULT_HOST_NAME);
    this.maxMapSlots = conf.getInt(
        "mapred.tasktracker.map.tasks.maximum", DEFAULT_MAP_SLOTS);
    this.maxReduceSlots = conf.getInt(
        "mapred.tasktracker.reduce.tasks.maximum", DEFAULT_REDUCE_SLOTS);
    this.heartbeatIntervalFuzz = conf.getInt(
        "mumak.tasktracker.heartbeat.fuzz", DEFAULT_HEARTBEAT_FUZZ);
    long seed = conf.getLong("mumak.tasktracker.random.seed", 
        System.nanoTime());
    this.random = new Random(seed);
  }
  
  /**
   * Processes a simulation event. 
   *
   * @param event the event to process, should be an instance of HeartbeatEvent
   *        or TaskAttemptCompletionEvent
   * @return the list of new events generated in response
   */
  @Override
  public List<SimulatorEvent> accept(SimulatorEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Accepted event " + event);
    }

    if (event instanceof HeartbeatEvent) {
      return processHeartbeatEvent((HeartbeatEvent)event);
    } else if (event instanceof TaskAttemptCompletionEvent) {
      return processTaskAttemptCompletionEvent((TaskAttemptCompletionEvent)
                                               event);
    } else {
      throw new IllegalArgumentException("Unhandled event "+event);
    }
  }
  
  /**
   * Called once at the start of the simulation.
   *
   * @param when Time when the task tracker starts.
   * @return the initial HeartbeatEvent for ourselves.
   */ 
  public List<SimulatorEvent> init(long when) {
    LOG.debug("TaskTracker starting up, current simulation time=" + when);

    return Collections.<SimulatorEvent>singletonList(new HeartbeatEvent(this, when));  
  }
 
  /**
   * Stops running a task attempt on the task tracker. It also updates the 
   * number of available slots accordingly.
   * 
   * @param finalStatus the TaskStatus containing the task id and final 
   *        status of the task attempt. This rountine asserts a lot of the
   *        finalStatus params, in case it is coming from a task attempt
   *        completion event sent to ourselves. Only the run state, finish time,
   *        and progress fields of the task attempt are updated.
   * @param now Current simulation time, used for assert only
   */
  private void finishRunningTask(TaskStatus finalStatus, long now) {
    TaskAttemptID taskId = finalStatus.getTaskID();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Finishing running task id=" + taskId + ", now=" + now);
    }

    SimulatorTaskInProgress tip = tasks.get(taskId);
    if (tip == null) {
      throw new IllegalArgumentException("Unknown task attempt " + taskId
          + " completed");
    }
    TaskStatus currentStatus = tip.getTaskStatus();
    if (currentStatus.getRunState() != State.RUNNING) {
      throw new IllegalArgumentException(
          "Task attempt to finish is not running: " + tip);
    }

    // Check that finalStatus describes a task attempt that has just been
    // completed
    State finalRunState = finalStatus.getRunState();
    if (finalRunState != State.SUCCEEDED && finalRunState != State.FAILED
        && finalRunState != State.KILLED) {
      throw new IllegalArgumentException(
          "Final run state for completed task can't be : " + finalRunState
              + " " + tip);
    }

    if (now != finalStatus.getFinishTime()) {
      throw new IllegalArgumentException(
          "Current time does not match task finish time: now=" + now
              + ", finish=" + finalStatus.getFinishTime());
    }

    if (currentStatus.getIsMap() != finalStatus.getIsMap()
        || currentStatus.getNumSlots() != finalStatus.getNumSlots()
        || currentStatus.getPhase() != finalStatus.getPhase()
        || currentStatus.getStartTime() != finalStatus.getStartTime()) {
      throw new IllegalArgumentException(
          "Current status does not match final status");
    }

    // We can't assert getShuffleFinishTime() and getSortFinishTime() for
    // reduces as those were unknown when the task attempt completion event
    // was created. We have not called setMapFinishTime() for maps either.
    // If we were really thorough we could update the progress of the task
    // and check if it is consistent with finalStatus.

    // If we've got this far it is safe to update the task status
    currentStatus.setRunState(finalStatus.getRunState());
    currentStatus.setFinishTime(finalStatus.getFinishTime());
    currentStatus.setProgress(finalStatus.getProgress());

    // and update the free slots
    int numSlots = currentStatus.getNumSlots();
    if (tip.isMapTask()) {
      usedMapSlots -= numSlots;
      if (usedMapSlots < 0) {
        throw new IllegalStateException(
            "TaskTracker reaches negative map slots: " + usedMapSlots);
      }
    } else {
      usedReduceSlots -= numSlots;
      if (usedReduceSlots < 0) {
        throw new IllegalStateException(
            "TaskTracker reaches negative reduce slots: " + usedReduceSlots);
      }
    }
  }

  /**
   * Records that a task attempt has completed. Ignores the event for tasks
   * that got killed after the creation of the completion event.
   * 
   * @param event the TaskAttemptCompletionEvent the tracker sent to itself
   * @return the list of response events, empty
   */ 
  private List<SimulatorEvent> processTaskAttemptCompletionEvent(
      TaskAttemptCompletionEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing task attempt completion event" + event);
    }
    
    long now = event.getTimeStamp();  
    TaskStatus finalStatus = event.getStatus();
    TaskAttemptID taskID = finalStatus.getTaskID();
    boolean killedEarlier = orphanTaskCompletions.remove(taskID);
    if (!killedEarlier) {
      finishRunningTask(finalStatus, now);
    }
    return SimulatorEngine.EMPTY_EVENTS;
  }
  
 
  /** 
   * Creates a signal for itself marking the completion of a task attempt. 
   * It assumes that the task attempt hasn't made any progress in the user 
   * space code so far, i.e. it is called right at launch for map tasks and
   * immediately after all maps completed for reduce tasks.
   *
   * @param tip the simulator task in progress
   * @param now the current simulation time
   * @return the TaskAttemptCompletionEvent we are sending to ourselves
   */
  private TaskAttemptCompletionEvent createTaskAttemptCompletionEvent(
                                       SimulatorTaskInProgress tip, long now) {
    // We need to clone() status as we modify and it goes into an Event
    TaskStatus status = (TaskStatus)tip.getTaskStatus().clone();
    long delta = tip.getUserSpaceRunTime();
    assert delta >= 0 : "TaskAttempt " + tip.getTaskStatus().getTaskID()
        + " has negative UserSpaceRunTime = " + delta;
    long finishTime = now + delta;
    status.setFinishTime(finishTime);
    if (tip.isMapTask()) {
      status.setMapFinishTime(finishTime);
    }
    status.setProgress(1.0f);
    status.setRunState(tip.getFinalRunState());
    TaskAttemptCompletionEvent event = 
        new TaskAttemptCompletionEvent(this, status);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Created task attempt completion event " + event);
    }
    return event;
  }
  
  /**
   * Launches a task on the simulated task tracker. 
   * 
   * @param action SimulatorLaunchTaskAction sent by the job tracker
   * @param now current simulation time
   * @return new events generated, a TaskAttemptCompletionEvent for map
   *         tasks, empty otherwise
   */
  private List<SimulatorEvent> handleSimulatorLaunchTaskAction(
                         SimulatorLaunchTaskAction action, long now) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling launch task action " + action);
    }
    // First, create statuses and update used slots for map and reduce 
    // task separately
    Task task = action.getTask();
    TaskAttemptID taskId = task.getTaskID();
    if (tasks.containsKey(taskId)) {
      throw new IllegalArgumentException("Multiple launch of task id =" + taskId);
    }

    // Ctor of MapTaskStatus and ReduceTaskStatus need deprecated 
    // o.a.h.mapred.TaskAttemptID, hence the downgrade
    org.apache.hadoop.mapred.TaskAttemptID taskIdOldApi = 
        org.apache.hadoop.mapred.TaskAttemptID.downgrade(taskId);
    TaskStatus status;
    int numSlotsRequired = task.getNumSlotsRequired();    
    Counters emptyCounters = new Counters();
    if (task.isMapTask()) {     
      status = new MapTaskStatus(taskIdOldApi, 0f, numSlotsRequired,
                                 State.RUNNING, "", "", taskTrackerName, 
                                 Phase.MAP, emptyCounters);
      usedMapSlots += numSlotsRequired;
      if (usedMapSlots > maxMapSlots) {
        throw new IllegalStateException("usedMapSlots exceeds maxMapSlots: " + 
          usedMapSlots + " > " + maxMapSlots);
      }
    } else {
      status = new ReduceTaskStatus(taskIdOldApi, 0f, numSlotsRequired, 
                                    State.RUNNING, "", "", taskTrackerName,
                                    Phase.SHUFFLE, emptyCounters);
      usedReduceSlots += numSlotsRequired;
      if (usedReduceSlots > maxReduceSlots) {
        throw new IllegalStateException("usedReduceSlots exceeds usedReduceSlots: " + 
            usedReduceSlots + " > " + usedReduceSlots);
      }
    }
    
    //  Second, create and store a TIP
    status.setStartTime(now);
    SimulatorTaskInProgress tip = 
      new SimulatorTaskInProgress(action, status, now);
    tasks.put(taskId, tip);

    // Third, schedule events for ourselves
    if (task.isMapTask()) {
      // we know when this task attempts ends iff it is a map 
      TaskAttemptCompletionEvent e = createTaskAttemptCompletionEvent(tip, now);
      return Collections.<SimulatorEvent>singletonList(e);
    } else { 
      // reduce, completion time can only be determined when all maps are done
      return SimulatorEngine.EMPTY_EVENTS;
    }
  }
  
  /** 
   * Kills a task attempt.
   *
   * @param action contains the task attempt to kill
   * @param now current simulation time
   * @return new events generated in response, empty
   */
  private List<SimulatorEvent> handleKillTaskAction(KillTaskAction action, long now) {
    TaskAttemptID taskId = action.getTaskID();
    // we don't have a nice(r) toString() in Hadoop's TaskActions 
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling kill task action, taskId=" + taskId + ", now=" + now);
    }
    
    SimulatorTaskInProgress tip = tasks.get(taskId);
    
    // Safety check: We might get a KillTaskAction even for completed reduces
    if (tip == null) {
      return SimulatorEngine.EMPTY_EVENTS;
    }
    
    progressTaskStatus(tip, now); // make progress up to date
    TaskStatus finalStatus = (TaskStatus)tip.getTaskStatus().clone();
    finalStatus.setFinishTime(now);
    finalStatus.setRunState(State.KILLED);
    finishRunningTask(finalStatus, now);
   
    if (finalStatus.getIsMap() || finalStatus.getPhase() == Phase.REDUCE) {
      // if we have already created a task attempt completion event we remember
      // the task id, so that we can safely ignore the event when its delivered
      orphanTaskCompletions.add(taskId);
    }
    return SimulatorEngine.EMPTY_EVENTS;
  }  

  /** 
   * Starts "running" the REDUCE phase of reduce upon being notified that 
   * all map tasks are (successfully) done.
   *
   * @param action contains the notification for one of the reduce tasks
   * @param now current simulation time
   * @return new events generated, a single TaskAttemptCompletionEvent for the
   *         reduce
   */
  private List<SimulatorEvent> handleAllMapsCompletedTaskAction(
                        AllMapsCompletedTaskAction action, long now) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling all maps completed task action " + action);
    }
    
    TaskAttemptID taskId = action.getTaskID();
    SimulatorTaskInProgress tip = tasks.get(taskId);
    // If tip is null here it is because the task attempt to be notified is
    // unknown to this TaskTracker.
    TaskStatus status = tip.getTaskStatus();
    if (status.getIsMap()) {
      throw new IllegalStateException(
          "Invalid AllMapsCompletedTaskAction, task attempt "
              + "to be notified is a map: " + taskId + " " + status);
    }
    if (status.getPhase() != Phase.SHUFFLE) {
      throw new IllegalArgumentException(
          "Reducer task attempt already notified: " + taskId + " " + status);
    }
           
    // Warning: setPhase() uses System.currentTimeMillis() internally to
    // set shuffle and sort times, but we overwrite that manually anyway
    status.setPhase(Phase.REDUCE);
    status.setShuffleFinishTime(now);
    status.setSortFinishTime(now);
    
    // Forecast the completion of this reduce
    TaskAttemptCompletionEvent e = createTaskAttemptCompletionEvent(tip, now);
    return Collections.<SimulatorEvent>singletonList(e);
  }
  
  /** 
   * Updates the progress indicator of a task if it is running.
   * 
   * @param tip simulator task in progress whose progress is to be updated
   * @param now current simulation time
   */
  private void progressTaskStatus(SimulatorTaskInProgress tip, long now) {
    TaskStatus status = tip.getTaskStatus();
    if (status.getRunState() != State.RUNNING) {
      return; // nothing to be done
    }

    boolean isMap = tip.isMapTask();
    // Time when the user space code started
    long startTime = -1;
    // Time spent in map or just in the REDUCE phase of a reduce task
    long runTime = tip.getUserSpaceRunTime();
    float progress = 0.0f;
    if (isMap) {
      // We linearly estimate the progress of maps since their start 
      startTime = status.getStartTime();
      progress = ((float)(now - startTime)) / runTime;
    } else {
      // We don't model reduce progress in the SHUFFLE or SORT phases
      // We use linear estimate for the 3rd, REDUCE phase
      Phase reducePhase = status.getPhase();
      switch (reducePhase) {
      case SHUFFLE:
        progress = 0.0f; // 0 phase is done out of 3
        break;
      case SORT:
        progress = 1.0f/3; // 1 phase is done out of 3
        break;
      case REDUCE: {
        // REDUCE phase with the user code started when sort finished
        startTime = status.getSortFinishTime();
        // 0.66f : 2 phases are done out of of 3
        progress = 2.0f/3 + (((float) (now - startTime)) / runTime) / 3.0f;
      }
        break;
      default:
        // should never get here
        throw new IllegalArgumentException("Invalid reducePhase=" + reducePhase);
      }
    }
    
    final float EPSILON = 0.0001f;
    if (progress < -EPSILON || progress > 1 + EPSILON) {
      throw new IllegalStateException("Task progress out of range: " + progress);
    }
    progress = Math.max(Math.min(1.0f, progress), 0.0f);
    status.setProgress(progress);
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Updated task progress, taskId=" + status.getTaskID()
          + ", progress=" + status.getProgress());
    }
  }
  
  /**
   * Updates the progress indicator of all running tasks. 
   *
   * @param now current simulation time
   */
  private void progressTaskStatuses(long now) {
    for (SimulatorTaskInProgress tip : tasks.values()) {
      progressTaskStatus(tip, now);
    }
  }

  /** 
   * Frees up bookkeping memory used by completed tasks. 
   * Has no effect on the events or logs produced by the SimulatorTaskTracker.
   * We need this in order not to report completed task multiple times and 
   * to ensure that we do not run out of Java heap memory in larger 
   * simulations.
   */
  private void garbageCollectCompletedTasks() {
    for (Iterator<TaskAttemptID> iter = tasks.keySet().iterator();
         iter.hasNext();) {
      TaskAttemptID taskId = iter.next();
      SimulatorTaskInProgress tip = tasks.get(taskId);
      if (tip.getTaskStatus().getRunState() != State.RUNNING) {
        iter.remove();
        if (LOG.isDebugEnabled()) {
          LOG.debug("Garbage collected SimulatorTIP, taskId=" + taskId);
        }
        // We don't have to / must not touch usedMapSlots and usedReduceSlots
        // as those were already updated by processTaskAttemptCompletionEvent() 
        // when the task switched its state from running
      }
    }
  }
  
  /**
   * Creates a list of task statuses suitable for transmission via heartbeat().
   * The task statuses are cloned() so that the heartbeat() callee, the job 
   * tracker, can't mess up the SimulatorTaskTracker's internal data.
   *
   * @return the list of running and recently completed task statuses 
   * on the tracker
   */
  private List<TaskStatus> collectAndCloneTaskStatuses() {
    ArrayList<TaskStatus> statuses = new ArrayList<TaskStatus>();
    for (SimulatorTaskInProgress tip : tasks.values()) {
      statuses.add((TaskStatus)tip.getTaskStatus().clone());
    }
    return statuses;
  }

  /**
   * Handles the HeartbeatResponse received from the job tracker upon 
   * heartbeat(). Dispatches to handle*Action() methods.
   *
   * @param response HeartbeatResponse received from the job tracker
   * @param now current simulation time
   * @return list of new events generated in response to the task actions
   */
  private List<SimulatorEvent> handleHeartbeatResponse(HeartbeatResponse response,
                                              long now) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling heartbeat response " + response);
    }

    List<SimulatorEvent> events = new ArrayList<SimulatorEvent>();  
    TaskTrackerAction[] actions = response.getActions();
    for (TaskTrackerAction action : actions) {
      List<SimulatorEvent> actionEvents;
      if (action instanceof SimulatorLaunchTaskAction) {
        actionEvents = handleSimulatorLaunchTaskAction(
            (SimulatorLaunchTaskAction)action, now);            
      } else if(action instanceof KillTaskAction) {
        actionEvents = handleKillTaskAction((KillTaskAction)action, now);
      } else if(action instanceof AllMapsCompletedTaskAction) {
        // our extra task action for notifying the reducers
        actionEvents = handleAllMapsCompletedTaskAction(
            (AllMapsCompletedTaskAction)action, now);
      } else {
        // Should never get here.
        // CommitTaskAction is not implemented in the simulator
        // LaunchTaskAction has to be SimulatorLaunchTaskAction
        throw new UnsupportedOperationException("Unimplemented TaskAction: "
            + action);
      }
      events.addAll(actionEvents);
    }
    return events;
  }
  
  /** 
   * Transmits a heartbeat event to the jobtracker and processes the response.
   *
   * @param event HeartbeatEvent to process
   * @return list of new events generated in response
   */
  private List<SimulatorEvent> processHeartbeatEvent(HeartbeatEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing heartbeat event " + event);
    }
    
    long now = event.getTimeStamp();
    
    // Create the TaskTrackerStatus to report
    progressTaskStatuses(now);
    List<TaskStatus> taskStatuses = collectAndCloneTaskStatuses();
    boolean askForNewTask = (usedMapSlots < maxMapSlots ||
                             usedReduceSlots < maxReduceSlots);

    // 0 means failures == 0 here. Undocumented in TaskTracker, but does not 
    // seem to be used at all in org.apache.hadoop.mapred .
    TaskTrackerStatus taskTrackerStatus = 
      new SimulatorTaskTrackerStatus(taskTrackerName, hostName, httpPort, 
          taskStatuses, 0, 
          maxMapSlots, maxReduceSlots, now);
    
    // This is the right, and only, place to release bookkeping memory held 
    // by completed tasks: after collectAndCloneTaskStatuses() and before 
    // heartbeat().
    // The status of TIPs to be purged is already cloned & copied to
    // taskStatuses for reporting
    // We shouldn't run the gc after heartbeat() since  KillTaskAction might 
    // produce new completed tasks that we have not yet reported back and 
    // don't want to purge immediately.
    garbageCollectCompletedTasks();
    
    // Transmit the heartbeat 
    HeartbeatResponse response = null;
    try {
      response = 
        jobTracker.heartbeat(taskTrackerStatus, false, firstHeartbeat, 
                             askForNewTask, heartbeatResponseId);
    } catch (IOException ioe) {
      throw new IllegalStateException("Internal error", ioe);
    }
    firstHeartbeat = false;

    // The heartbeat got through successfully!
    heartbeatResponseId = response.getResponseId();

    // Process the heartbeat response
    List<SimulatorEvent> events = handleHeartbeatResponse(response, now);
    
    // Next heartbeat
    int heartbeatInterval = response.getHeartbeatInterval();
    if (heartbeatIntervalFuzz > 1) {
      // Add some randomness to heartbeat timings to simulate network latency, 
      // time spent servicing this heartbeat request, etc.
      // randomFuzz is in (-heartbeatIntervalFuzz,+heartbeatIntervalFuzz)
      int randomFuzz = random.nextInt(2*heartbeatIntervalFuzz-1) - 
                       heartbeatIntervalFuzz;
      heartbeatInterval += randomFuzz;
      // make sure we never schedule a heartbeat in the past
      heartbeatInterval = Math.max(1, heartbeatInterval); 
    }
    events.add(new HeartbeatEvent(this, now + heartbeatInterval));

    return events;
  }
  
  /**
   * Internal helper class used for storing the current status and other
   * auxilliary information associated with a task attempt assigned to
   * a simulator task tracker.
   * WARNING: This is a completely different inner class than the one with
   *          the same name in SimulatorJobTracker.
   */
  static class SimulatorTaskInProgress {
  
    /**
     * Current status of the task attempt. 
     * We store the start time, the start time of reduce phases and the
     * run state of the task in this object.
     */
    private TaskStatus taskStatus;
    
    /** 
     * Object storing the run time and the final state of the task attempt.
     * It is never read directly by the SimulatorTaskTracker.
     */
    private TaskAttemptInfo taskAttempInfo;

    /**
     * Runtime of the user-space code of the task attempt. This is the full
     * runtime for map tasks, and only that of the REDUCE phase for reduce
     * tasks.
     */ 
    private final long userSpaceRunTime;
    
    /** 
     * Constructs an object by copying most of the fields from a
     * SimulatorTaskAction.
     */
    public SimulatorTaskInProgress(SimulatorLaunchTaskAction action,
                                   TaskStatus taskStatus, long now) {
      this.taskStatus = taskStatus;
      this.taskAttempInfo = action.getTaskAttemptInfo();
      if (taskStatus.getIsMap()) {
        this.userSpaceRunTime = taskAttempInfo.getRuntime();
      } else {
        this.userSpaceRunTime = 
          ((ReduceTaskAttemptInfo)taskAttempInfo).getReduceRuntime();
      }
    }
    
    /**
     * Returns whether the task attempt is a map. 
     * 
     * @return true iff the task attempt is a map
     */
    public boolean isMapTask() {
      return taskStatus.getIsMap();
    }

    /*
     * Returns the current status of the task attempt. 
     *
     * @return current task status
     */
    public TaskStatus getTaskStatus() {
      return taskStatus;
    }
    
    /** 
     * Sets the status of the task attempt.
     *
     * @param status the new task status
     */
    public void setTaskStatus(TaskStatus status) {
      this.taskStatus = status;
    }
        
    /** 
     * Returns the final state of the completed task.
     * 
     * @return the final state of the completed task; 
     *        it is either State.SUCCEEDED or State.FAILED
     */
    public State getFinalRunState() {
      return taskAttempInfo.getRunState();
    }
    
    /**
     * Gets the time spent in the user space code of the task attempt.
     * This is the full runtime for map tasks, and only that of the REDUCE 
     * phase for reduce tasks.
     *
     * @return the user space runtime 
     */
    public long getUserSpaceRunTime() {
      return userSpaceRunTime;
    }          
  }
}
