/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.io.WritableUtils;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Test the {@link TaskID} class.
 */
public class TestTaskID {
  /**
   * Test of getJobID method, of class TaskID.
   */
  @Test
  public void testGetJobID() {
    JobID jobId = new JobID("1234", 0);
    TaskID taskId = new TaskID(jobId, TaskType.MAP, 0);

    assertSame("TaskID did not store the JobID correctly",
        jobId, taskId.getJobID());

    taskId = new TaskID();

    assertEquals("Job ID was set unexpectedly in default contsructor",
        "", taskId.getJobID().getJtIdentifier());
  }

  /**
   * Test of isMap method, of class TaskID.
   */
  @Test
  public void testIsMap() {
    JobID jobId = new JobID("1234", 0);

    for (TaskType type : TaskType.values()) {
      TaskID taskId = new TaskID(jobId, type, 0);

      if (type == TaskType.MAP) {
        assertTrue("TaskID for map task did not correctly identify itself "
            + "as a map task", taskId.isMap());
      } else {
        assertFalse("TaskID for " + type + " task incorrectly identified "
            + "itself as a map task", taskId.isMap());
      }
    }

    TaskID taskId = new TaskID();

    assertFalse("TaskID of default type incorrectly identified itself as a "
        + "map task", taskId.isMap());
  }

  /**
   * Test of getTaskType method, of class TaskID.
   */
  @Test
  public void testGetTaskType0args() {
    JobID jobId = new JobID("1234", 0);

    for (TaskType type : TaskType.values()) {
      TaskID taskId = new TaskID(jobId, type, 0);

      assertEquals("TaskID incorrectly reported its type",
          type, taskId.getTaskType());
    }

    TaskID taskId = new TaskID();

    assertEquals("TaskID of default type incorrectly reported its type",
        TaskType.REDUCE, taskId.getTaskType());
  }

  /**
   * Test of equals method, of class TaskID.
   */
  @Test
  public void testEquals() {
    JobID jobId1 = new JobID("1234", 1);
    JobID jobId2 = new JobID("2345", 2);
    TaskID taskId1 = new TaskID(jobId1, TaskType.MAP, 0);
    TaskID taskId2 = new TaskID(jobId1, TaskType.MAP, 0);

    assertTrue("The equals() method reported two equal task IDs were not equal",
        taskId1.equals(taskId2));

    taskId2 = new TaskID(jobId2, TaskType.MAP, 0);

    assertFalse("The equals() method reported two task IDs with different "
        + "job IDs were equal", taskId1.equals(taskId2));

    taskId2 = new TaskID(jobId1, TaskType.MAP, 1);

    assertFalse("The equals() method reported two task IDs with different IDs "
        + "were equal", taskId1.equals(taskId2));

    TaskType[] types = TaskType.values();

    for (int i = 0; i < types.length; i++) {
      for (int j = 0; j < types.length; j++) {
        taskId1 = new TaskID(jobId1, types[i], 0);
        taskId2 = new TaskID(jobId1, types[j], 0);

        if (i == j) {
          assertTrue("The equals() method reported two equal task IDs were not "
              + "equal", taskId1.equals(taskId2));
        } else {
          assertFalse("The equals() method reported two task IDs with "
              + "different types were equal", taskId1.equals(taskId2));
        }
      }
    }

    assertFalse("The equals() method matched against a JobID object",
        taskId1.equals(jobId1));

    assertFalse("The equals() method matched against a null object",
        taskId1.equals(null));
  }

  /**
   * Test of compareTo method, of class TaskID.
   */
  @Test
  public void testCompareTo() {
    JobID jobId = new JobID("1234", 1);
    TaskID taskId1 = new TaskID(jobId, TaskType.REDUCE, 0);
    TaskID taskId2 = new TaskID(jobId, TaskType.REDUCE, 0);

    assertEquals("The compareTo() method returned non-zero for two equal "
        + "task IDs", 0, taskId1.compareTo(taskId2));

    taskId2 = new TaskID(jobId, TaskType.MAP, 1);

    assertTrue("The compareTo() method did not weigh task type more than task "
        + "ID", taskId1.compareTo(taskId2) > 0);

    TaskType[] types = TaskType.values();

    for (int i = 0; i < types.length; i++) {
      for (int j = 0; j < types.length; j++) {
        taskId1 = new TaskID(jobId, types[i], 0);
        taskId2 = new TaskID(jobId, types[j], 0);

        if (i == j) {
          assertEquals("The compareTo() method returned non-zero for two equal "
              + "task IDs", 0, taskId1.compareTo(taskId2));
        } else if (i < j) {
          assertTrue("The compareTo() method did not order " + types[i]
              + " before " + types[j], taskId1.compareTo(taskId2) < 0);
        } else {
          assertTrue("The compareTo() method did not order " + types[i]
              + " after " + types[j], taskId1.compareTo(taskId2) > 0);
        }
      }
    }

    try {
      taskId1.compareTo(jobId);
      fail("The compareTo() method allowed comparison to a JobID object");
    } catch (ClassCastException ex) {
      // Expected
    }

    try {
      taskId1.compareTo(null);
      fail("The compareTo() method allowed comparison to a null object");
    } catch (NullPointerException ex) {
      // Expected
    }
  }

  /**
   * Test of toString method, of class TaskID.
   */
  @Test
  public void testToString() {
    JobID jobId = new JobID("1234", 1);

    for (TaskType type : TaskType.values()) {
      TaskID taskId = new TaskID(jobId, type, 0);
      String str = String.format("task_1234_0001_%c_000000",
          TaskID.getRepresentingCharacter(type));

      assertEquals("The toString() method returned the wrong value",
          str, taskId.toString());
    }
  }

  /**
   * Test of appendTo method, of class TaskID.
   */
  @Test
  public void testAppendTo() {
    JobID jobId = new JobID("1234", 1);
    StringBuilder builder = new StringBuilder();

    for (TaskType type : TaskType.values()) {
      builder.setLength(0);
      TaskID taskId = new TaskID(jobId, type, 0);
      String str = String.format("_1234_0001_%c_000000",
          TaskID.getRepresentingCharacter(type));

      assertEquals("The appendTo() method appended the wrong value",
          str, taskId.appendTo(builder).toString());
    }

    try {
      new TaskID().appendTo(null);
      fail("The appendTo() method allowed a null builder");
    } catch (NullPointerException ex) {
      // Expected
    }
  }

  /**
   * Test of hashCode method, of class TaskID.
   */
  @Test
  public void testHashCode() {
    TaskType[] types = TaskType.values();

    for (int i = 0; i < types.length; i++) {
      JobID jobId = new JobID("1234" + i, i);
      TaskID taskId1 = new TaskID(jobId, types[i], i);
      TaskID taskId2 = new TaskID(jobId, types[i], i);

      assertTrue("The hashcode() method gave unequal hash codes for two equal "
          + "task IDs", taskId1.hashCode() == taskId2.hashCode());
    }
  }

  /**
   * Test of readFields method, of class TaskID.
   */
  @Test
  public void testReadFields() throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    out.writeInt(0);
    out.writeInt(1);
    WritableUtils.writeVInt(out, 4);
    out.write(new byte[] {0x31, 0x32, 0x33, 0x34});
    WritableUtils.writeEnum(out, TaskType.REDUCE);

    DataInputByteBuffer in = new DataInputByteBuffer();

    in.reset(ByteBuffer.wrap(baos.toByteArray()));

    TaskID instance = new TaskID();

    instance.readFields(in);

    assertEquals("The readFields() method did not produce the expected task ID",
        "task_1234_0001_r_000000", instance.toString());
  }

  /**
   * Test of write method, of class TaskID.
   */
  @Test
  public void testWrite() throws Exception {
    JobID jobId = new JobID("1234", 1);
    TaskID taskId = new TaskID(jobId, TaskType.JOB_SETUP, 0);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(baos);

    taskId.write(out);

    DataInputByteBuffer in = new DataInputByteBuffer();
    byte[] buffer = new byte[4];

    in.reset(ByteBuffer.wrap(baos.toByteArray()));

    assertEquals("The write() method did not write the expected task ID",
        0, in.readInt());
    assertEquals("The write() method did not write the expected job ID",
        1, in.readInt());
    assertEquals("The write() method did not write the expected job "
        + "identifier length", 4, WritableUtils.readVInt(in));
    in.readFully(buffer, 0, 4);
    assertEquals("The write() method did not write the expected job "
        + "identifier length", "1234", new String(buffer));
    assertEquals("The write() method did not write the expected task type",
        TaskType.JOB_SETUP, WritableUtils.readEnum(in, TaskType.class));
  }

  /**
   * Test of forName method, of class TaskID.
   */
  @Test
  public void testForName() {
    assertEquals("The forName() method did not parse the task ID string "
        + "correctly", "task_1_0001_m_000000",
        TaskID.forName("task_1_0001_m_000").toString());
    assertEquals("The forName() method did not parse the task ID string "
        + "correctly", "task_23_0002_r_000001",
        TaskID.forName("task_23_0002_r_0001").toString());
    assertEquals("The forName() method did not parse the task ID string "
        + "correctly", "task_345_0003_s_000002",
        TaskID.forName("task_345_0003_s_00002").toString());
    assertEquals("The forName() method did not parse the task ID string "
        + "correctly", "task_6789_0004_c_000003",
        TaskID.forName("task_6789_0004_c_000003").toString());
    assertEquals("The forName() method did not parse the task ID string "
        + "correctly", "task_12345_0005_t_4000000",
        TaskID.forName("task_12345_0005_t_4000000").toString());

    try {
      TaskID.forName("tisk_12345_0005_t_4000000");
      fail("The forName() method parsed an invalid job ID: "
          + "tisk_12345_0005_t_4000000");
    } catch (IllegalArgumentException ex) {
      // Expected
    }

    try {
      TaskID.forName("tisk_12345_0005_t_4000000");
      fail("The forName() method parsed an invalid job ID: "
          + "tisk_12345_0005_t_4000000");
    } catch (IllegalArgumentException ex) {
      // Expected
    }

    try {
      TaskID.forName("task_abc_0005_t_4000000");
      fail("The forName() method parsed an invalid job ID: "
          + "task_abc_0005_t_4000000");
    } catch (IllegalArgumentException ex) {
      // Expected
    }

    try {
      TaskID.forName("task_12345_xyz_t_4000000");
      fail("The forName() method parsed an invalid job ID: "
          + "task_12345_xyz_t_4000000");
    } catch (IllegalArgumentException ex) {
      // Expected
    }

    try {
      TaskID.forName("task_12345_0005_x_4000000");
      fail("The forName() method parsed an invalid job ID: "
          + "task_12345_0005_x_4000000");
    } catch (IllegalArgumentException ex) {
      // Expected
    }

    try {
      TaskID.forName("task_12345_0005_t_jkl");
      fail("The forName() method parsed an invalid job ID: "
          + "task_12345_0005_t_jkl");
    } catch (IllegalArgumentException ex) {
      // Expected
    }

    try {
      TaskID.forName("task_12345_0005_t");
      fail("The forName() method parsed an invalid job ID: "
          + "task_12345_0005_t");
    } catch (IllegalArgumentException ex) {
      // Expected
    }

    try {
      TaskID.forName("task_12345_0005_4000000");
      fail("The forName() method parsed an invalid job ID: "
          + "task_12345_0005_4000000");
    } catch (IllegalArgumentException ex) {
      // Expected
    }

    try {
      TaskID.forName("task_12345_t_4000000");
      fail("The forName() method parsed an invalid job ID: "
          + "task_12345_t_4000000");
    } catch (IllegalArgumentException ex) {
      // Expected
    }

    try {
      TaskID.forName("12345_0005_t_4000000");
      fail("The forName() method parsed an invalid job ID: "
          + "12345_0005_t_4000000");
    } catch (IllegalArgumentException ex) {
      // Expected
    }
  }

  /**
   * Test of getRepresentingCharacter method, of class TaskID.
   */
  @Test
  public void testGetRepresentingCharacter() {
    assertEquals("The getRepresentingCharacter() method did not return the "
        + "expected character", 'm',
        TaskID.getRepresentingCharacter(TaskType.MAP));
    assertEquals("The getRepresentingCharacter() method did not return the "
        + "expected character", 'r',
        TaskID.getRepresentingCharacter(TaskType.REDUCE));
    assertEquals("The getRepresentingCharacter() method did not return the "
        + "expected character", 's',
        TaskID.getRepresentingCharacter(TaskType.JOB_SETUP));
    assertEquals("The getRepresentingCharacter() method did not return the "
        + "expected character", 'c',
        TaskID.getRepresentingCharacter(TaskType.JOB_CLEANUP));
    assertEquals("The getRepresentingCharacter() method did not return the "
        + "expected character", 't',
        TaskID.getRepresentingCharacter(TaskType.TASK_CLEANUP));
  }

  /**
   * Test of getTaskType method, of class TaskID.
   */
  @Test
  public void testGetTaskTypeChar() {
    assertEquals("The getTaskType() method did not return the expected type",
        TaskType.MAP,
        TaskID.getTaskType('m'));
    assertEquals("The getTaskType() method did not return the expected type",
        TaskType.REDUCE,
        TaskID.getTaskType('r'));
    assertEquals("The getTaskType() method did not return the expected type",
        TaskType.JOB_SETUP,
        TaskID.getTaskType('s'));
    assertEquals("The getTaskType() method did not return the expected type",
        TaskType.JOB_CLEANUP,
        TaskID.getTaskType('c'));
    assertEquals("The getTaskType() method did not return the expected type",
        TaskType.TASK_CLEANUP,
        TaskID.getTaskType('t'));
    assertNull("The getTaskType() method did not return null for an unknown "
        + "type", TaskID.getTaskType('x'));
  }

  /**
   * Test of getAllTaskTypes method, of class TaskID.
   */
  @Test
  public void testGetAllTaskTypes() {
    assertEquals("The getAllTaskTypes method did not return the expected "
        + "string", "(m|r|s|c|t)", TaskID.getAllTaskTypes());
  }
}
