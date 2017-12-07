import Ember from "ember";
import { PARTITION_LABEL } from "../constants";

export default Ember.Component.extend({
  didUpdateAttrs: function({oldAttrs, newAttrs}) {
    this._super(...arguments);
    this.set('data', this.initData());
  },

  init() {
    this._super(...arguments);
    this.set('data', this.initData());
  },

  initData() {
    const queue = this.get("queue");
    const partitionMap = this.get("partitionMap");
    const filteredParition = this.get("filteredPartition") || PARTITION_LABEL;
    const userLimit = queue.get("userLimit");
    const userLimitFactor = queue.get("userLimitFactor");
    const isLeafQueue = queue.get("isLeafQueue");
    
    return {
      ...partitionMap[filteredParition],
      userLimit,
      userLimitFactor,
      isLeafQueue
    };
  }
    
});
