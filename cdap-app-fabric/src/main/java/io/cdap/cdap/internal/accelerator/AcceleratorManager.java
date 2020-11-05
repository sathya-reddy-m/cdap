/*
 * Copyright © 2020 Cask Data, Inc.
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

package io.cdap.cdap.internal.accelerator;

import com.google.inject.Inject;
import io.cdap.cdap.api.metadata.MetadataEntity;
import io.cdap.cdap.api.metadata.MetadataScope;
import io.cdap.cdap.common.conf.CConfiguration;
import io.cdap.cdap.common.conf.Constants;
import io.cdap.cdap.metadata.MetadataAdmin;
import io.cdap.cdap.proto.id.ApplicationId;
import io.cdap.cdap.proto.id.NamespaceId;
import io.cdap.cdap.spi.metadata.MetadataRecord;
import io.cdap.cdap.spi.metadata.ScopedName;
import io.cdap.cdap.spi.metadata.SearchRequest;
import io.cdap.cdap.spi.metadata.SearchResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Class with helpful methods for dynamic accelerator framework
 */
public class AcceleratorManager {

  private static final Logger LOG = LoggerFactory.getLogger(AcceleratorManager.class);
  private static final String ACCELERATOR_TAG = "accelerator:%s";
  private static final String APPLICATION_TAG = "application:%s";
  private static final String ACCELERATOR_KEY = "accelerator";
  private final MetadataAdmin metadataAdmin;
  private final CConfiguration cConf;

  @Inject
  AcceleratorManager(MetadataAdmin metadataAdmin, CConfiguration cConf) {
    this.metadataAdmin = metadataAdmin;
    this.cConf = cConf;
  }

  /**
   * Returns the list of applications that are having metadata tagged with the accelerator
   *
   * @param namespace   - Namespace for which applications should be listed
   * @param accelerator - Accelerator by which to filter
   * @param cursor      - Optional cursor from a previous response
   * @param offset      - Offset from where to start
   * @param limit       - Limit of records to fetch
   * @return
   * @throws Exception - Exception from meta data search if any
   */
  public AcceleratorApplications getAppsForAccelerator(NamespaceId namespace, String accelerator,
                                                       @Nullable String cursor, int offset,
                                                       int limit) throws Exception {
    String acceleratorTag = String.format(ACCELERATOR_TAG, accelerator);
    SearchRequest searchRequest = SearchRequest.of(acceleratorTag).addNamespace(namespace.getNamespace())
      .setScope(MetadataScope.SYSTEM)
      .setCursor(cursor)
      .setOffset(offset)
      .setLimit(limit)
      .build();
    SearchResponse searchResponse = metadataAdmin.search(searchRequest);
    Set<ApplicationId> applicationIds = searchResponse.getResults().stream()
      .map(MetadataRecord::getEntity)
      .filter(this::isApplicationType)
      .map(this::getApplicationId)
      .collect(Collectors.toSet());
    return new AcceleratorApplications(applicationIds, searchResponse.getCursor(), searchResponse.getOffset(),
                                       searchResponse.getLimit(), searchResponse.getTotalResults());
  }

  /**
   * Returns boolean indicating whether application is disabled due to a disabled accelerator
   *
   * @param namespace
   * @param applicationName
   * @return
   * @throws Exception
   */
  public boolean isApplicationDisabled(String namespace, String applicationName) throws Exception {
    String applicationQuery = String.format(APPLICATION_TAG, applicationName);
    SearchResponse searchResponse = metadataAdmin
      .search(SearchRequest.of(applicationQuery).addNamespace(namespace).setScope(MetadataScope.SYSTEM).build());
    return searchResponse.getResults().stream()
      .filter(this::hasAcceleratorTagValue)
      .map(this::getAcceleratorTagValue)
      .anyMatch(this::isAcceleratorDisabled);
  }

  @Nullable
  private String getAcceleratorTagValue(MetadataRecord metadataRecord) {
    return metadataRecord.getMetadata().getProperties().get(new ScopedName(MetadataScope.SYSTEM, ACCELERATOR_KEY));
  }

  private boolean hasAcceleratorTagValue(MetadataRecord metadataRecord) {
    return getAcceleratorTagValue(metadataRecord) != null;
  }

  private boolean isApplicationType(MetadataEntity metadataEntity) {
    return MetadataEntity.APPLICATION.equals(metadataEntity.getType());
  }

  private ApplicationId getApplicationId(MetadataEntity metadataEntity) {
    return new ApplicationId(metadataEntity.getValue(MetadataEntity.NAMESPACE),
                             metadataEntity.getValue(MetadataEntity.APPLICATION),
                             metadataEntity.getValue(MetadataEntity.VERSION));
  }

  private boolean isAcceleratorDisabled(@Nullable String accelerator) {
    if (accelerator == null || accelerator.isEmpty()) {
      return false;
    }
    String enabledAccelerators = cConf.get(Constants.AppFabric.ENABLED_ACCELERATORS_LIST);
    if (enabledAccelerators == null || enabledAccelerators.isEmpty()) {
      return true;
    }
    return Arrays.stream(enabledAccelerators.split(","))
      .noneMatch(enabledAccelerator -> enabledAccelerator.equalsIgnoreCase(accelerator));
  }

  /**
   * Class for holding search results of a search for applications annotated with accelerators
   */
  public static class AcceleratorApplications {

    private final Set<ApplicationId> applicationIds;
    private final String cursor;
    private final int offset;
    private final int limit;
    private final int totalResults;

    public AcceleratorApplications(Set<ApplicationId> applicationIds, @Nullable String cursor,
                                   int offset, int limit, int totalResults) {
      this.applicationIds = applicationIds;
      this.cursor = cursor;
      this.offset = offset;
      this.limit = limit;
      this.totalResults = totalResults;
    }

    /**
     * @return {@link Set} of @link{ApplicationId}
     */
    public Set<ApplicationId> getApplicationIds() {
      return applicationIds;
    }

    /**
     * @return {@link String} cursor from search result if supported by backend
     */
    public String getCursor() {
      return cursor;
    }

    /**
     * @return int offset that was used in the search
     */
    public int getOffset() {
      return offset;
    }

    /**
     * @return int limit that was used in the search
     */
    public int getLimit() {
      return limit;
    }

    /**
     * @return int with total number of results or an estimate
     */
    public int getTotalResults() {
      return totalResults;
    }
  }
}
