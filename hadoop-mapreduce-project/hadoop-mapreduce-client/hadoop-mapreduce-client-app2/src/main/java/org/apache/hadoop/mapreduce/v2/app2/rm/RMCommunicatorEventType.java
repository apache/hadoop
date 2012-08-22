package org.apache.hadoop.mapreduce.v2.app2.rm;

public enum RMCommunicatorEventType {
  // TODO Essentialy the same as ContainerAllocator.
  // TODO XXX: Clean this up. CONTAINER_REQ and CONTAINER_FAILED not used.
  CONTAINER_REQ,
  CONTAINER_DEALLOCATE,
  CONTAINER_FAILED
}
