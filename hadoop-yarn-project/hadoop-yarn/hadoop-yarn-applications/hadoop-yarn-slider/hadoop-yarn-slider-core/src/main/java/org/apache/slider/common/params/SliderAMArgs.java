/*
 * Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.apache.slider.common.params;

/**
 * Parameters sent by the Client to the AM
 */
public class SliderAMArgs extends CommonArgs {

  SliderAMCreateAction createAction = new SliderAMCreateAction();

  public SliderAMArgs(String[] args) {
    super(args);
  }

  @Override
  protected void addActionArguments() {
    addActions(createAction);
  }

  public String getImage() {
    return createAction.image;
  }

  /**
   * This is the URI in the FS to the Slider cluster; the conf file (and any
   * other cluster-specifics) can be picked up here
   */
  public String getSliderClusterURI() {
    return createAction.sliderClusterURI;
  }

  /**
   * Am binding is simple: there is only one action
   */
  @Override
  public void applyAction() {
    bindCoreAction(createAction);
  }
}
