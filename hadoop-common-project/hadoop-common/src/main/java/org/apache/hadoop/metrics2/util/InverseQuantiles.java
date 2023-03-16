package org.apache.hadoop.metrics2.util;

import org.apache.hadoop.util.Preconditions;
import java.util.ListIterator;

public class InverseQuantiles extends SampleQuantiles{

  public InverseQuantiles(Quantile[] quantiles) {
    super(quantiles);
  }
  

  /**
   * Get the estimated value at the inverse of the specified quantile. 
   * Eg: return the value at (1 - 0.99)*count position for quantile 0.99.
   * When count is 100, quantile 0.99 is desired to return the value at the 1st position
   *
   * @param quantile Queried quantile, e.g. 0.50 or 0.99.
   * @return Estimated value at the inverse position of that quantile. 
   */
  long query(double quantile) {
    Preconditions.checkState(!samples.isEmpty(), "no data in estimator");

    int rankMin = 0;
    int desired = (int) ((1 - quantile) * count);

    ListIterator<SampleItem> it = samples.listIterator();
    SampleItem prev;
    SampleItem cur = it.next();
    for (int i = 1; i < samples.size(); i++) {
      prev = cur;
      cur = it.next();

      rankMin += prev.g;

      if (rankMin + cur.g + cur.delta > desired + (allowableError(i) / 2)) {
        return prev.value;
      }
    }

    // edge case of wanting max value
    return samples.get(0).value;
  }
}
