/**
 * Copyright 2005 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import java.io.*;

/** 
 * Includes details on a running MapReduce job.  A client can
 * track a living job using this object.
 *
 * @author Mike Cafarella
 */
public interface RunningJob {
    /**
     * Returns an identifier for the job
     */
    public String getJobID();

    /**
     * Returns the path of the submitted job.
     */
    public String getJobFile();

    /**
     * Returns a URL where some job progress information will be displayed.
     */
    public String getTrackingURL();

    /**
     * Returns a float between 0.0 and 1.0, indicating progress on
     * the map portion of the job.  When all map tasks have completed,
     * the function returns 1.0.
     */
    public float mapProgress() throws IOException;

    /**
     * Returns a float between 0.0 and 1.0, indicating progress on
     * the reduce portion of the job.  When all reduce tasks have completed,
     * the function returns 1.0.
     */
    public float reduceProgress() throws IOException;

    /**
     * Non-blocking function to check whether the job is finished or not.
     */
    public boolean isComplete() throws IOException;

    /**
     * True iff job completed successfully.
     */
    public boolean isSuccessful() throws IOException;

    /**
     * Blocks until the job is complete.
     */
    public void waitForCompletion() throws IOException;

    /**
     * Kill the running job.  Blocks until all job tasks have been
     * killed as well.  If the job is no longer running, it simply returns.
     */
    public void killJob() throws IOException;
}
