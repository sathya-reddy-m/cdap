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

import com.google.gson.Gson;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.etl.proto.v2.ETLConfig;
import io.cdap.cdap.spi.data.StructuredRow;
import io.cdap.cdap.spi.data.StructuredTable;
import io.cdap.cdap.spi.data.StructuredTableContext;
import io.cdap.cdap.spi.data.TableNotFoundException;
import io.cdap.cdap.spi.data.table.StructuredTableId;
import io.cdap.cdap.spi.data.table.StructuredTableSpecification;
import io.cdap.cdap.spi.data.table.field.Field;
import io.cdap.cdap.spi.data.table.field.Fields;
import io.cdap.cdap.spi.data.table.field.Range;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;


/**
 * Schema for draft store.
 */
public class DraftStore {
  public static final StructuredTableId TABLE_ID = new StructuredTableId("drafts");
  private static final Gson GSON = new Gson();
  public static final String NAMESPACE_COL = "namespace";
  public static final String GENERATION_COL = "generation";
  public static final String UUID_COL = "uuid";
  public static final String TYPE_COL = "type";
  public static final String NAME_COL = "name";
  public static final String CREATED_COL = "created";
  public static final String UPDATED_COL = "updated";
  public static final String PIPELINE_COL = "pipeline";
  public static final String REVISION_COL = "revision";

  public static final StructuredTableSpecification TABLE_SPEC = new StructuredTableSpecification.Builder()
    .withId(TABLE_ID)
    .withFields(Fields.stringType(NAMESPACE_COL),
                Fields.longType(GENERATION_COL),
                Fields.stringType(UUID_COL),
                Fields.stringType(TYPE_COL),
                Fields.stringType(NAME_COL),
                Fields.longType(CREATED_COL),
                Fields.longType(UPDATED_COL),
                Fields.stringType(PIPELINE_COL),
                Fields.intType(REVISION_COL))
    .withPrimaryKeys(NAMESPACE_COL, GENERATION_COL, UUID_COL)
    .withIndexes(TYPE_COL)
    .build();

  private final StructuredTable table;

  private DraftStore(StructuredTable table) {
    this.table = table;
  }

  public static DraftStore get(StructuredTableContext context) {
    try {
      StructuredTable table = context.getTable(TABLE_ID);
      return new DraftStore(table);
    } catch (TableNotFoundException e) {
      throw new IllegalStateException(String.format(
        "System table '%s' does not exist. Please check your system environment.", TABLE_ID.getName()), e);
    }
  }

  public List<Draft> listDrafts(NamespaceSummary namespace) throws IOException {
    List<Field<?>> prefix = new ArrayList<>(2);
    prefix.add(Fields.stringField(NAMESPACE_COL, namespace.getName()));
    prefix.add(Fields.longField(GENERATION_COL, namespace.getGeneration()));
    Range range = Range.singleton(prefix);
    List<Draft> results = new ArrayList<>();
    try (CloseableIterator<StructuredRow> rowIter = table.scan(range, Integer.MAX_VALUE)) {
      while (rowIter.hasNext()) {
        results.add(fromRow(rowIter.next()));
      }
    }
    return results;
  }

  public Optional<Draft> getDraft(DraftId id) throws IOException {
    Optional<StructuredRow> row = table.read(getKey(id));
    return row.map(this::fromRow);
  }

  public void deleteDraft(DraftId id) throws IOException {
    table.delete(getKey(id));
  }

  public void writeDraft(DraftId id, DraftRequest draftRequest) throws IOException {
    Optional<Draft> existing = getDraft(id);
    long now = System.currentTimeMillis();
    long createTime = existing.map(Draft::getCreatedTimeMillis).orElse(now);

    //TODO: Automatically populate revision number instead of hard-coded value
//    table.upsert(getRow(id, new Draft(id.getId(), draftRequest.getId(), draftRequest.getConfig(),
//                                      0, existing., createTime, now)));
  }

  private void addKeyFields(DraftId id, List<Field<?>> fields) {
    fields.add(Fields.stringField(NAMESPACE_COL, id.getNamespace().getName()));
    fields.add(Fields.longField(GENERATION_COL, id.getNamespace().getGeneration()));
    fields.add(Fields.stringField(NAME_COL, id.getId()));
  }

  private List<Field<?>> getKey(DraftId id) {
    List<Field<?>> keyFields = new ArrayList<>(3);
    addKeyFields(id, keyFields);
    return keyFields;
  }

  private List<Field<?>> getRow(DraftId id, Draft draft) {
    List<Field<?>> fields = new ArrayList<>(6);
    addKeyFields(id, fields);
//    fields.add(Fields.stringField(LABEL_COL, draft.getId()));
    fields.add(Fields.longField(CREATED_COL, draft.getCreatedTimeMillis()));
    fields.add(Fields.longField(UPDATED_COL, draft.getUpdatedTimeMillis()));
//    fields.add(Fields.bytesField(CONFIG_COL, GSON.toJson(draft.getConfig()).getBytes(StandardCharsets.UTF_8)));
    return fields;
  }

  @SuppressWarnings("ConstantConditions")
  private Draft fromRow(StructuredRow row) {
//    String label = row.getString(LABEL_COL);
    long createTime = row.getLong(CREATED_COL);
    long updateTime = row.getLong(UPDATED_COL);
    String configStr = new String(row.getBytes(PIPELINE_COL), StandardCharsets.UTF_8);
    ETLConfig config = GSON.fromJson(configStr, ETLConfig.class);
    return null;
//    return new Draft(row.getString(NAME_COL), label, config, revision, type, createTime, updateTime);
  }
}
