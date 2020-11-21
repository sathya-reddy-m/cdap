/*
 * Copyright © 2019 Cask Data, Inc.
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
 *
 */

package io.cdap.cdap.datapipeline.service;

import io.cdap.cdap.api.service.AbstractSystemService;
import io.cdap.cdap.datapipeline.draft.DraftStore;

/**
 * Service that handles pipeline studio operations, like validation and schema propagation.
 */
public class StudioService extends AbstractSystemService {
  public static final String NAME = "studio";

//  private final TransactionRunner txRunner;

//  public StudioService(TransactionRunner transactionRunner) {
//    this.txRunner = transactionRunner;
//  }


  @Override
  protected void configure() {
    setName(NAME);
    setDescription("Handles pipeline studio operations, like validation and schema propagation.");
    addHandler(new ValidationHandler());
    addHandler(new DraftHandler());
    createTable(DraftStore.TABLE_SPEC);
  }

//  /**
//   * List all drafts in the given namespace. If no drafts exist, an empty list is returned.
//   *
//   * @param namespace namespace to list drafts in
//   * @return list of drafts in the namespace
//   */
//  public List<Draft> listDrafts(NamespaceSummary namespace) {
//    return TransactionRunners.run(txRunner, context -> {
//      return DraftStore.get(context).listDrafts(namespace);
//    });
//  }
//
//  /**
//   * Get the draft for the given id, or throw an exception if it does not exist.
//   *
//   * @param draftId the id of the draft
//   * @return draft information
//   * @throws DraftNotFoundException if the draft does not exist
//   */
//  public Draft getDraft(DraftId draftId) {
//    Optional<Draft> draft = TransactionRunners.run(txRunner, context -> {
//      return DraftStore.get(context).getDraft(draftId);
//    });
//
//    return draft.orElseThrow(() -> new DraftNotFoundException(draftId));
//  }
//
//  /**
//   * Store the contents of a pipeline config as a draft. If a draft already exists, it's modified time is updated and
//   * the config is overwritten. If none exists, a new draft is created.
//   *
//   *
//   *
//   * @param draftId if of the draft to save
//   * @param draft draft to save
//   */
//  public void saveDraft(DraftId draftId, DraftRequest draft) {
////    try {
////      draft.getConfig().validateDraft();
////    } catch (IllegalArgumentException e) {
////      throw new InvalidDraftException(e.getMessage(), e);
////    }
//    TransactionRunners.run(txRunner, context -> {
//      DraftStore draftStore = DraftStore.get(context);
//      draftStore.writeDraft(draftId, draft);
//    });
//  }
//
//  /**
//   * Delete the given draft.
//   *
//   * @param draftId id of the draft to delete
//   */
//  public void deleteDraft(DraftId draftId) {
//    TransactionRunners.run(txRunner, context -> {
//      DraftStore draftStore = DraftStore.get(context);
//      draftStore.deleteDraft(draftId);
//    });
//  }

}
