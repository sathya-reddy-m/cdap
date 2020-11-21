/*
 * Copyright © 2020 Cask Data, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package io.cdap.cdap.datapipeline.draft;


import io.cdap.cdap.etl.proto.v2.ETLConfig;

import java.util.Objects;

/**
 * Request to store a draft.
 */
public class DraftRequest {
  private final String id;
  private final ETLConfig config;
  private final String previousHash;
  private final String name;
  private final int revision;
  private final String type;


  public DraftRequest(String id, ETLConfig config, String previousHash, String name, int revision, String type) {
    this.id = id;
    this.config = config;
    this.previousHash = previousHash;
    this.name = name;
    this.revision = revision;
    this.type = type;
  }

  public String getName() {
    return name == null ? "" : name;
  }

  public int getRevision() {
    return revision;
  }

  public String getType() {
    return type;
  }

  public String getId() {
    return id;
  }

  public ETLConfig getConfig() {
    return config;
  }

  public String getPreviousHash() {
    return previousHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DraftRequest that = (DraftRequest) o;
    return Objects.equals(id, that.id) &&
      Objects.equals(config, that.config) &&
      Objects.equals(previousHash, that.previousHash) &&
      Objects.equals(name, that.name) &&
      Objects.equals(type, that.type) &&
      revision == that.revision;
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, config, previousHash, revision, type, name);
  }


}

