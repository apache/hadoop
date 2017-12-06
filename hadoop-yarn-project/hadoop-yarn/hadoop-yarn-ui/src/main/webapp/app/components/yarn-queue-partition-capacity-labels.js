import Ember from "ember";
import { PARTITION_LABEL } from "../constants";

export default Ember.Component.extend({
  filteredCapacity: Ember.computed("content", function() {
    const queue = this.get("queue");
    const partitionMap = this.get("partitionMap");
    const filteredParition = this.get("filteredParition") || PARTITION_LABEL;
    const userLimit = queue.get("userLimit");
    const userLimitFactor = queue.get("userLimitFactor");
    const isLeafQueue = queue.get("isLeafQueue");

    return {
      ...partitionMap[filteredParition],
      userLimit,
      userLimitFactor,
      isLeafQueue
    };
  })
});
