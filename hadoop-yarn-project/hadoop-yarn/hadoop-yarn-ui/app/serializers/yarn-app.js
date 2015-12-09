import DS from 'ember-data';
import Converter from 'yarn-ui/utils/converter';

export default DS.JSONAPISerializer.extend({
    internalNormalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      if (payload.app) {
        payload = payload.app;  
      }
      
      var fixedPayload = {
        id: id,
        type: primaryModelClass.modelName, // yarn-app
        attributes: {
          appName: payload.name,
          user: payload.user,
          queue: payload.queue,
          state: payload.state,
          startTime: Converter.timeStampToDate(payload.startedTime),
          elapsedTime: Converter.msToElapsedTime(payload.elapsedTime),
          finishedTime: Converter.timeStampToDate(payload.finishedTime),
          finalStatus: payload.finalStatus,
          progress: payload.progress,
          diagnostics: payload.diagnostics,
          amContainerLogs: payload.amContainerLogs,
          amHostHttpAddress: payload.amHostHttpAddress,
          logAggregationStatus: payload.logAggregationStatus,
          unmanagedApplication: payload.unmanagedApplication,
          amNodeLabelExpression: payload.amNodeLabelExpression,
          priority: payload.priority,
          allocatedMB: payload.allocatedMB,
          allocatedVCores: payload.allocatedVCores,
          runningContainers: payload.runningContainers,
          memorySeconds: payload.memorySeconds,
          vcoreSeconds: payload.vcoreSeconds,
          preemptedResourceMB: payload.preemptedResourceMB,
          preemptedResourceVCores: payload.preemptedResourceVCores,
          numNonAMContainerPreempted: payload.numNonAMContainerPreempted,
          numAMContainerPreempted: payload.numAMContainerPreempted
        }
      };

      return fixedPayload;
    },

    normalizeSingleResponse(store, primaryModelClass, payload, id,
      requestType) {
      var p = this.internalNormalizeSingleResponse(store, 
        primaryModelClass, payload, id, requestType);
      return { data: p };
    },

    normalizeArrayResponse(store, primaryModelClass, payload, id,
      requestType) {
      // return expected is { data: [ {}, {} ] }
      var normalizedArrayResponse = {};

      // payload has apps : { app: [ {},{},{} ]  }
      // need some error handling for ex apps or app may not be defined.
      normalizedArrayResponse.data = payload.apps.app.map(singleApp => {
        return this.internalNormalizeSingleResponse(store, primaryModelClass,
          singleApp, singleApp.id, requestType);
      }, this);
      return normalizedArrayResponse;
    }
});