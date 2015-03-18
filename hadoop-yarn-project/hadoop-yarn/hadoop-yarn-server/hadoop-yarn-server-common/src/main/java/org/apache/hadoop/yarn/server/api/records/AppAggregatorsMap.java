package org.apache.hadoop.yarn.server.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.util.Records;


@Private
public abstract class AppAggregatorsMap {

  public static AppAggregatorsMap newInstance(
      ApplicationId id, String aggregatorAddr) {
    AppAggregatorsMap appAggregatorMap =
        Records.newRecord(AppAggregatorsMap.class);
    appAggregatorMap.setApplicationId(id);
    appAggregatorMap.setAggregatorAddr(aggregatorAddr);
    return appAggregatorMap;
  }
  
  public abstract ApplicationId getApplicationId();
  
  public abstract void setApplicationId(
      ApplicationId id);
  
  public abstract String getAggregatorAddr();
  
  public abstract void setAggregatorAddr(
      String addr);

}
