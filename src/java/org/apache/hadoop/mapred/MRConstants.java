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

/*******************************
 * Some handy constants
 * 
 * @author Mike Cafarella
 *******************************/
interface MRConstants {
    //
    // Timeouts, constants
    //
    public static final long HEARTBEAT_INTERVAL = 10 * 1000;
    public static final long TASKTRACKER_EXPIRY_INTERVAL = 10 * 60 * 1000;

    //
    // Result codes
    //
    public static int SUCCESS = 0;
    public static int FILE_NOT_FOUND = -1;
}
