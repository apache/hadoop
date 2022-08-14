package org.apache.hadoop.yarn.server.federation.store.records;

import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RouterRMDTSecretManagerState {

  // DTIdentifier -> renewDate
  private Map<RMDelegationTokenIdentifier, Long> delegationTokenState = new HashMap<RMDelegationTokenIdentifier, Long>();

  private Set<DelegationKey> masterKeyState = new HashSet<DelegationKey>();

  private int dtSequenceNumber = 0;

  public Map<RMDelegationTokenIdentifier, Long> getTokenState() {
    return delegationTokenState;
  }

  public Set<DelegationKey> getMasterKeyState() {
    return masterKeyState;
  }

  public int getDTSequenceNumber() {
    return dtSequenceNumber;
  }

  public void setDtSequenceNumber(int dtSequenceNumber) {
    this.dtSequenceNumber = dtSequenceNumber;
  }
}
