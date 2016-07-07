/*
 * Copyright © 2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.cdap.api.customaction;

import java.util.Map;

/**
 * This abstract class provides a default implementation of {@link CustomAction} methods for easy extensions.
 */
public abstract class AbstractCustomAction implements CustomAction {

  private final String name;
  private CustomActionConfigurer configurer;
  private CustomActionContext context;

  protected AbstractCustomAction() {
    name = getClass().getSimpleName();
  }

  protected AbstractCustomAction(String name) {
    this.name = name;
  }

  @Override
  public void configure(CustomActionConfigurer configurer) {
    this.configurer = configurer;
    setName(name);
    configure();
  }

  protected void configure() {

  }

  protected void setName(String name) {
    configurer.setName(name);
  }

  protected void setDescription(String description) {
    configurer.setDescription(description);
  }

  protected void setProperties(Map<String, String> properties) {
    configurer.setProperties(properties);
  }

  @Override
  public void initialize(CustomActionContext context) throws Exception {
    this.context = context;
  }

  @Override
  public void destroy() {

  }

  protected final CustomActionContext getContext() {
    return context;
  }
}
