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
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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

    assertSame(jobId, taskId.getJobID(),
        "TaskID did not store the JobID correctly");

    taskId = new TaskID();

    assertEquals("", taskId.getJobID().getJtIdentifier(),
        "Job ID was set unexpectedly in default constructor");
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
        assertTrue(taskId.isMap(),
            "TaskID for map task did not correctly identify itself as a map task");
      } else {
        assertFalse(taskId.isMap(),
            "TaskID for " + type + " task incorrectly identified itself as a map task");
      }
    }

    TaskID taskId = new TaskID();

    assertFalse(taskId.isMap(),
        "TaskID of default type incorrectly identified itself as a map task");
  }

  /**
   * Test of getTaskType method, of class TaskID.
   */
  @Test
  public void testGetTaskType0args() {
    JobID jobId = new JobID("1234", 0);

    for (TaskType type : TaskType.values()) {
      TaskID taskId = new TaskID(jobId, type, 0);

      assertEquals(type, taskId.getTaskType(),
          "TaskID incorrectly reported its type");
    }

    TaskID taskId = new TaskID();

    assertEquals(TaskType.REDUCE, taskId.getTaskType(),
        "TaskID of default type incorrectly reported its type");
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

    assertTrue(taskId1.equals(taskId2),
        "The equals() method reported two equal task IDs were not equal");

    taskId2 = new TaskID(jobId2, TaskType.MAP, 0);

    assertFalse(taskId1.equals(taskId2),
        "The equals() method reported two task IDs with different " +
        "job IDs were equal");

    taskId2 = new TaskID(jobId1, TaskType.MAP, 1);

    assertFalse(taskId1.equals(taskId2),
        "The equals() method reported two task IDs with different IDs " +
        "were equal");

    TaskType[] types = TaskType.values();

    for (int i = 0; i < types.length; i++) {
      for (int j = 0; j < types.length; j++) {
        taskId1 = new TaskID(jobId1, types[i], 0);
        taskId2 = new TaskID(jobId1, types[j], 0);

        if (i == j) {
          assertTrue(taskId1.equals(taskId2),
              "The equals() method reported two equal task IDs were not equal");
        } else {
          assertFalse(taskId1.equals(taskId2),
              "The equals() method reported two task IDs with " +
              "different types were equal");
        }
      }
    }

    assertFalse(taskId1.equals(jobId1),
        "The equals() method matched against a JobID object");

    assertFalse(taskId1.equals(null),
        "The equals() method matched against a null object");
  }

  /**
   * Test of compareTo method, of class TaskID.
   */
  @Test
  public void testCompareTo() {
    JobID jobId = new JobID("1234", 1);
    TaskID taskId1 = new TaskID(jobId, TaskType.REDUCE, 0);
    TaskID taskId2 = new TaskID(jobId, TaskType.REDUCE, 0);

    assertEquals(0, taskId1.compareTo(taskId2),
        "The compareTo() method returned non-zero for two equal task IDs");

    taskId2 = new TaskID(jobId, TaskType.MAP, 1);

    assertTrue(taskId1.compareTo(taskId2) > 0,
        "The compareTo() method did not weigh task type more than task ID");

    TaskType[] types = TaskType.values();

    for (int i = 0; i < types.length; i++) {
      for (int j = 0; j < types.length; j++) {
        taskId1 = new TaskID(jobId, types[i], 0);
        taskId2 = new TaskID(jobId, types[j], 0);

        if (i == j) {
          assertEquals(0, taskId1.compareTo(taskId2),
              "The compareTo() method returned non-zero for two equal task IDs");
        } else if (i < j) {
          assertTrue(taskId1.compareTo(taskId2) < 0,
              "The compareTo() method did not order " + types[i]
              + " before " + types[j]);
        } else {
          assertTrue(taskId1.compareTo(taskId2) > 0,
              "The compareTo() method did not order " + types[i]
              + " after " + types[j]);
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

      assertEquals(str, taskId.toString(),
          "The toString() method returned the wrong value");
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

      assertEquals(str, taskId.appendTo(builder).toString(),
          "The appendTo() method appended the wrong value.");
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

      assertEquals(taskId1.hashCode(), taskId2.hashCode(),
          "The hashcode() method gave unequal hash codes for two equal task IDs");
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

    assertEquals("task_1234_0001_r_000000", instance.toString(),
        "The readFields() method did not produce the expected task ID");
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

    assertEquals(0, in.readInt(),
        "The write() method did not write the expected task ID");
    assertEquals(1, in.readInt(),
        "The write() method did not write the expected job ID");
    assertEquals(4, WritableUtils.readVInt(in),
        "The write() method did not write the expected job identifier length");
    in.readFully(buffer, 0, 4);
    assertEquals("1234", new String(buffer),
        "The write() method did not write the expected job identifier length");
    assertEquals(TaskType.JOB_SETUP, WritableUtils.readEnum(in, TaskType.class),
        "The write() method did not write the expected task type");
  }

  /**
   * Test of forName method, of class TaskID.
   */
  @Test
  public void testForName() {
    assertEquals("task_1_0001_m_000000", TaskID.forName("task_1_0001_m_000").toString(),
        "The forName() method did not parse the task ID string correctly");
    assertEquals("task_23_0002_r_000001", TaskID.forName("task_23_0002_r_0001").toString(),
        "The forName() method did not parse the task ID string correctly");
    assertEquals("task_345_0003_s_000002", TaskID.forName("task_345_0003_s_00002").toString(),
        "The forName() method did not parse the task ID string correctly");
    assertEquals("task_6789_0004_c_000003", TaskID.forName("task_6789_0004_c_000003").toString(),
        "The forName() method did not parse the task ID string correctly");
    assertEquals("task_12345_0005_t_4000000",
        TaskID.forName("task_12345_0005_t_4000000").toString(),
        "The forName() method did not parse the task ID string correctly");

    try {
      TaskID.forName("tisk_12345_0005_t_4000000");
      fail("The forName() method parsed an invalid job ID: tisk_12345_0005_t_4000000");
    } catch (IllegalArgumentException ex) {
      // Expected
    }

    try {
      TaskID.forName("tisk_12345_0005_t_4000000");
      fail("The forName() method parsed an invalid job ID: tisk_12345_0005_t_4000000");
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
    assertEquals('m', TaskID.getRepresentingCharacter(TaskType.MAP),
        "The getRepresentingCharacter() method did not return the expected character");
    assertEquals('r', TaskID.getRepresentingCharacter(TaskType.REDUCE),
        "The getRepresentingCharacter() method did not return the " +
        "expected character");
    assertEquals('s', TaskID.getRepresentingCharacter(TaskType.JOB_SETUP),
        "The getRepresentingCharacter() method did not return the "
        + "expected character");
    assertEquals('c', TaskID.getRepresentingCharacter(TaskType.JOB_CLEANUP),
        "The getRepresentingCharacter() method did not return the expected character");
    assertEquals('t', TaskID.getRepresentingCharacter(TaskType.TASK_CLEANUP),
        "The getRepresentingCharacter() method did not return the expected character");
  }

  /**
   * Test of getTaskType method, of class TaskID.
   */
  @Test
  public void testGetTaskTypeChar() {
    assertEquals(TaskType.MAP, TaskID.getTaskType('m'),
        "The getTaskType() method did not return the expected type");
    assertEquals(TaskType.REDUCE, TaskID.getTaskType('r'),
        "The getTaskType() method did not return the expected type");
    assertEquals(TaskType.JOB_SETUP, TaskID.getTaskType('s'),
        "The getTaskType() method did not return the expected type");
    assertEquals(TaskType.JOB_CLEANUP,
        TaskID.getTaskType('c'), "The getTaskType() method did not return the expected type");
    assertEquals(TaskType.TASK_CLEANUP,
        TaskID.getTaskType('t'), "The getTaskType() method did not return the expected type");
    assertNull(TaskID.getTaskType('x'),
        "The getTaskType() method did not return null for an unknown type");
  }

  /**
   * Test of getAllTaskTypes method, of class TaskID.
   */
  @Test
  public void testGetAllTaskTypes() {
    assertEquals("(m|r|s|c|t)", TaskID.getAllTaskTypes(),
        "The getAllTaskTypes method did not return the expected string");
  }
}
