import DS from 'ember-data';

export default DS.JSONAPISerializer.extend({

    normalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      var children = [];
      if (payload.queues) {
        payload.queues.queue.forEach(function(queue) {
          children.push(queue.queueName);
        });
      }

      var includedData = [];
      var relationshipUserData = [];

      // update user models
      if (payload.users && payload.users.user) {
        payload.users.user.forEach(function(u) {
          includedData.push({
            type: "YarnUser",
            id: u.username + "_" + payload.queueName,
            attributes: {
              name: u.username,
              queueName: payload.queueName,
              usedMemoryMB: u.resourcesUsed.memory || 0,
              usedVCore: u.resourcesUsed.vCores || 0,
            }
          });

          relationshipUserData.push({
            type: "YarnUser",
            id: u.username + "_" + payload.queueName,
          })
        });
      }


      var fixedPayload = {
        id: id,
        type: primaryModelClass.modelName, // yarn-queue
        attributes: {
          name: payload.queueName,
          parent: payload.myParent,
          children: children,
          capacity: payload.capacity,
          usedCapacity: payload.usedCapacity,
          maxCapacity: payload.maxCapacity,
          absCapacity: payload.absoluteCapacity,
          absMaxCapacity: payload.absoluteMaxCapacity,
          absUsedCapacity: payload.absoluteUsedCapacity,
          state: payload.state,
          userLimit: payload.userLimit,
          userLimitFactor: payload.userLimitFactor,
          preemptionDisabled: payload.preemptionDisabled,
          numPendingApplications: payload.numPendingApplications,
          numActiveApplications: payload.numActiveApplications,
        },
        // Relationships
        relationships: {
          users: {
            data: relationshipUserData
          }
        }
      };

      return {
        queue: this._super(store, primaryModelClass, fixedPayload, id, requestType),
        includedData: includedData
      }
    },

    handleQueue(store, primaryModelClass, payload, id, requestType) {
      var data = [];
      var includedData = []
      var result = this.normalizeSingleResponse(store, primaryModelClass,
        payload, id, requestType);

      data.push(result.queue);
      includedData = includedData.concat(result.includedData);

      if (payload.queues) {
        for (var i = 0; i < payload.queues.queue.length; i++) {
          var queue = payload.queues.queue[i];
          queue.myParent = payload.queueName;
          var childResult = this.handleQueue(store, primaryModelClass, queue,
            queue.queueName,
            requestType);

          data = data.concat(childResult.data);
          includedData = includedData.concat(childResult.includedData);
        }
      }

      return {
        data: data,
        includedData, includedData
      }
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id,
      requestType) {
      var normalizedArrayResponse = {};
      var result = this.handleQueue(store,
        primaryModelClass,
        payload.scheduler.schedulerInfo, "root", requestType);

      normalizedArrayResponse.data = result.data;
      normalizedArrayResponse.included = result.includedData;

      console.log(normalizedArrayResponse);

      return normalizedArrayResponse;

      /*
      // return expected is { data: [ {}, {} ] }
      var normalizedArrayResponse = {};

      // payload has apps : { app: [ {},{},{} ]  }
      // need some error handling for ex apps or app may not be defined.
      normalizedArrayResponse.data = payload.apps.app.map(singleApp => { 
        return this.normalizeSingleResponse(store, primaryModelClass, singleApp, singleApp.id, requestType);
      }, this);
      return normalizedArrayResponse;
      */
    }
});