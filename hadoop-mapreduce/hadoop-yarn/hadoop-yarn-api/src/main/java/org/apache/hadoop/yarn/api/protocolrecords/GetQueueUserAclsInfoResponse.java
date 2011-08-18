package org.apache.hadoop.yarn.api.protocolrecords;

import java.util.List;

import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;

public interface GetQueueUserAclsInfoResponse {

  public List<QueueUserACLInfo> getUserAclsInfoList();
  
  public void setUserAclsInfoList(List<QueueUserACLInfo> queueUserAclsList);
  
}
