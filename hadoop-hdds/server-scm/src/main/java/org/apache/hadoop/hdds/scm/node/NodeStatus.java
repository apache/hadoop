package org.apache.hadoop.hdds.scm.node;

import org.apache.hadoop.hdds.protocol.proto.HddsProtos;

import java.util.Objects;

public class NodeStatus {

  private HddsProtos.NodeOperationalState operationalState;
  private HddsProtos.NodeState health;

  NodeStatus(HddsProtos.NodeOperationalState operationalState,
             HddsProtos.NodeState health) {
    this.operationalState = operationalState;
    this.health = health;
  }

  public HddsProtos.NodeState getHealth() {
    return health;
  }

  public HddsProtos.NodeOperationalState getOperationalState() {
    return operationalState;
  }

  public void setOperationalState(
      HddsProtos.NodeOperationalState newOperationalState) {
    assert newOperationalState != null;
    operationalState = newOperationalState;
  }

  public void setHealth(HddsProtos.NodeState newHealth) {
    assert newHealth != null;
    health = newHealth;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    NodeStatus other = (NodeStatus) obj;
    if (this.operationalState == other.operationalState &&
        this.health == other.health)
      return true;
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hash(health, operationalState);
  }

}
