/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

(function(global) {
  "use strict";

  function element(target) {
    if (typeof target === "string") {
      return document.querySelector(target);
    }
    if (target && target.jquery) {
      return target[0];
    }
    return target;
  }

  function component(type, target) {
    var node = element(target);
    return node ? global.bootstrap[type].getOrCreateInstance(node) : null;
  }

  global.hadoopBootstrap = {
    showModal: function(target) {
      component("Modal", target).show();
    },
    hideModal: function(target) {
      component("Modal", target).hide();
    },
    showTab: function(target) {
      component("Tab", target).show();
    },
    disposePopovers: function(selector) {
      document.querySelectorAll(selector).forEach(function(node) {
        var popover = global.bootstrap.Popover.getInstance(node);
        if (popover) {
          var tipId = node.getAttribute("aria-describedby");
          popover.dispose();
          node.removeAttribute("aria-describedby");
          if (tipId) {
            var tip = document.getElementById(tipId);
            if (tip) {
              tip.remove();
            }
          }
        }
      });
    },
    showPopover: function(target, options) {
      var node = element(target);
      var popover = global.bootstrap.Popover.getOrCreateInstance(node, options);
      popover.show();
      return popover;
    },
    setButtonBusy: function(target, busy) {
      var node = element(target);
      if (!node) {
        return;
      }
      if (!node.dataset.hadoopOriginalText) {
        node.dataset.hadoopOriginalText = node.textContent;
      }
      node.textContent = busy ? node.dataset.completeText : node.dataset.hadoopOriginalText;
    }
  };
})(window);
