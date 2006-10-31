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

package org.apache.hadoop.mapred.jobcontrol;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

/** This class encapsulates a set of MapReduce jobs and its dependency. It tracks 
 *  the states of the jobs by placing them into different tables according to their 
 *  states. 
 *  
 *  This class provides APIs for the client app to add a job to the group and to get 
 *  the jobs in the group in different states. When a 
 *  job is added, an ID unique to the group is assigned to the job. 
 *  
 *  This class has a thread that submits jobs when they become ready, monitors the
 *  states of the running jobs, and updates the states of jobs based on the state changes 
 *  of their depending jobs states. The class provides APIs for suspending/resuming
 *  the thread,and for stopping the thread.
 *  
 */
public class JobControl implements Runnable{

	// The thread can be in one of the following state
	private static final int RUNNING = 0;
	private static final int SUSPENDED = 1;
	private static final int STOPPED = 2;
	private static final int STOPPING = 3;
	private static final int READY = 4;
	
	private int runnerState;			// the thread state
	
	private Hashtable waitingJobs;
	private Hashtable readyJobs;
	private Hashtable runningJobs;
	private Hashtable successfulJobs;
	private Hashtable failedJobs;
	
	private long nextJobID;
	private String groupName;
	
	/** 
	 * Construct a job control for a group of jobs.
	 * @param groupName a name identifying this group
	 */
	public JobControl(String groupName) {
		this.waitingJobs = new Hashtable();
		this.readyJobs = new Hashtable();
		this.runningJobs = new Hashtable();
		this.successfulJobs = new Hashtable();
		this.failedJobs = new Hashtable();
		this.nextJobID = -1;
		this.groupName = groupName;
		this.runnerState = JobControl.READY;
		
	}
	
	private static ArrayList toArrayList(Hashtable jobs) {
		ArrayList retv = new ArrayList();
		Iterator iter = jobs.values().iterator();
		while (iter.hasNext()) {
			retv.add(iter.next());
		}
		return retv;
	}
	
	/**
	 * @return the jobs in the waiting state
	 */
	public ArrayList getWaitingJobs() {
		return JobControl.toArrayList(this.waitingJobs);
	}
	
	/**
	 * @return the jobs in the running state
	 */
	public ArrayList getRunningJobs() {
		return JobControl.toArrayList(this.runningJobs);
	}
	
	/**
	 * @return the jobs in the ready state
	 */
	public ArrayList getReadyJobs() {
		return JobControl.toArrayList(this.readyJobs);
	}
	
	/**
	 * @return the jobs in the success state
	 */
	public ArrayList getSuccessfulJobs() {
		return JobControl.toArrayList(this.successfulJobs);
	}
	
	public ArrayList getFailedJobs() {
		return JobControl.toArrayList(this.failedJobs);
	}
	
	private String getNextJobID() {
		nextJobID += 1;
		return this.groupName + this.nextJobID;
	}
	
	private static void addToQueue(Job aJob, Hashtable queue) {
		synchronized(queue) {
			queue.put(aJob.getJobID(), aJob);
		}		
	}
	
	private void addToQueue(Job aJob) {
		Hashtable queue = getQueue(aJob.getState());
		addToQueue(aJob, queue);	
	}
	
	private Hashtable getQueue(int state) {
		Hashtable retv = null;
		if (state == Job.WAITING) {
			retv = this.waitingJobs;
		} else if (state == Job.READY) {
			retv = this.readyJobs;
		} else if (state == Job.RUNNING) {
			retv = this.runningJobs;
		} else if (state == Job.SUCCESS) {
			retv = this.successfulJobs;
		} else if (state == Job.FAILED || state == Job.DEPENDENT_FAILED) {
			retv = this.failedJobs;
		} 
		return retv;
			
	}

	/**
	 * Add a new job.
	 * @param aJob the the new job
	 */
	synchronized public String addJob(Job aJob) {
		String id = this.getNextJobID();
		aJob.setJobID(id);
		aJob.setState(Job.WAITING);
		this.addToQueue(aJob);
		return id;	
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	/**
	 * @return the thread state
	 */
	public int getState() {
		return this.runnerState;
	}
	
	/**
	 * set the thread state to STOPPING so that the 
	 * thread will stop when it wakes up.
	 */
	public void stop() {
		this.runnerState = JobControl.STOPPING;
	}
	
	/**
	 * suspend the running thread
	 */
	public void suspend () {
		if (this.runnerState == JobControl.RUNNING) {
			this.runnerState = JobControl.SUSPENDED;
		}
	}
	
	/**
	 * resume the suspended thread
	 */
	public void resume () {
		if (this.runnerState == JobControl.SUSPENDED) {
			this.runnerState = JobControl.RUNNING;
		}
	}
	
	synchronized private void checkRunningJobs() {
		
		Hashtable oldJobs = null;
		oldJobs = this.runningJobs;
		this.runningJobs = new Hashtable();
		
		Iterator jobs = oldJobs.values().iterator();
		while (jobs.hasNext()) {
			Job nextJob = (Job)jobs.next();
			int state = nextJob.checkState();
			/*
			if (state != Job.RUNNING) {
				System.out.println("The state of the running job " +
					nextJob.getJobName() + " has changed to: " + nextJob.getState());
			}
			*/
			this.addToQueue(nextJob);
		}
	}
	
	synchronized private void checkWaitingJobs() {
		Hashtable oldJobs = null;
		oldJobs = this.waitingJobs;
		this.waitingJobs = new Hashtable();
		
		Iterator jobs = oldJobs.values().iterator();
		while (jobs.hasNext()) {
			Job nextJob = (Job)jobs.next();
			int state = nextJob.checkState();
			/*
			if (state != Job.WAITING) {
				System.out.println("The state of the waiting job " +
					nextJob.getJobName() + " has changed to: " + nextJob.getState());
			}
			*/
			this.addToQueue(nextJob);
		}
	}
	
	synchronized private void startReadyJobs() {
		Hashtable oldJobs = null;
		oldJobs = this.readyJobs;
		this.readyJobs = new Hashtable();
		
		Iterator jobs = oldJobs.values().iterator();
		while (jobs.hasNext()) {
			Job nextJob = (Job)jobs.next();
			//System.out.println("Job to submit to Hadoop: " + nextJob.getJobName());
			nextJob.submit();
			//System.out.println("Hadoop ID: " + nextJob.getMapredJobID());
			this.addToQueue(nextJob);
		}	
	}
	
	synchronized public boolean allFinished() {
		return this.waitingJobs.size() == 0 &&
			this.readyJobs.size() == 0 &&
			this.runningJobs.size() == 0;
	}
	
	/**
	 *  The main loop for the thread.
	 *  The loop does the following:
	 *  	Check the states of the running jobs
	 *  	Update the states of waiting jobs
	 *  	Submit the jobs in ready state
	 */
	public void run() {
		this.runnerState = JobControl.RUNNING;
		while (true) {
			while (this.runnerState == JobControl.SUSPENDED) {
				try {
					Thread.sleep(5000);
				}
				catch (Exception e) {
					
				}
			}
			checkRunningJobs();	
			checkWaitingJobs();		
			startReadyJobs();		
			if (this.runnerState != JobControl.RUNNING && 
					this.runnerState != JobControl.SUSPENDED) {
				break;
			}
			try {
				Thread.sleep(5000);
			}
			catch (Exception e) {
				
			}
			if (this.runnerState != JobControl.RUNNING && 
					this.runnerState != JobControl.SUSPENDED) {
				break;
			}
		}
		this.runnerState = JobControl.STOPPED;
	}

}
