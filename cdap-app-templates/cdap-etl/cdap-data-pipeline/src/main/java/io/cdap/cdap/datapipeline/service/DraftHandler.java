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

package io.cdap.cdap.datapipeline.service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import io.cdap.cdap.api.NamespaceSummary;
import io.cdap.cdap.api.service.http.AbstractSystemHttpServiceHandler;
import io.cdap.cdap.api.service.http.HttpServiceRequest;
import io.cdap.cdap.api.service.http.HttpServiceResponder;
import io.cdap.cdap.api.service.http.SystemHttpServiceContext;
import io.cdap.cdap.datapipeline.draft.CodedException;
import io.cdap.cdap.datapipeline.draft.Draft;
import io.cdap.cdap.datapipeline.draft.DraftId;
import io.cdap.cdap.datapipeline.draft.DraftNotFoundException;
import io.cdap.cdap.datapipeline.draft.DraftRequest;
import io.cdap.cdap.etl.api.Engine;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.proto.v2.ETLBatchConfig;
import io.cdap.cdap.etl.proto.v2.ETLPlugin;
import io.cdap.cdap.etl.proto.v2.ETLStage;
import io.cdap.cdap.spi.data.TableNotFoundException;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Handler of drafts
 */
public class DraftHandler extends AbstractSystemHttpServiceHandler {
  private static final Gson GSON = new GsonBuilder()
    .setPrettyPrinting()
    .create();

  private static HashMap<String, Draft> drafts = new HashMap<String, Draft>() {{
    put("test", new Draft("Test Draft",
      "test",
      ETLBatchConfig.builder()
                    .addStage(new ETLStage("source",
                      new ETLPlugin("Mock", BatchSource.PLUGIN_TYPE, new HashMap<>(), null)))
                    .addStage(new ETLStage("sink", new ETLPlugin("Mock", BatchSink.PLUGIN_TYPE,
                      new HashMap<>(), null)))
                    .addConnection("source", "sink")
                    .setEngine(Engine.SPARK)
                    .build(),
      0,
      "BATCH",
      System.currentTimeMillis(),
      System.currentTimeMillis()));
  }};

  @GET
  @Path("v1/contexts/{context}/drafts")
  public void listDrafts(HttpServiceRequest request, HttpServiceResponder responder,
    @PathParam("context") String namespaceName) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      responder.sendJson(drafts.values());
    });
  }

  @GET
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void getDraft(HttpServiceRequest request, HttpServiceResponder responder,
    @PathParam("context") String namespaceName,
    @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      if (!drafts.containsKey(draftName)) {
        throw new DraftNotFoundException(new DraftId(new NamespaceSummary(namespaceName, "", 1), draftName));
      }

      Draft draft = drafts.get(draftName);
      responder.sendJson(draft);
    });
  }

  @PUT
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void putDraft(HttpServiceRequest request, HttpServiceResponder responder,
    @PathParam("context") String namespaceName,
    @PathParam("draft") String draftId) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      DraftRequest draft;
      try {
        draft = GSON.fromJson(StandardCharsets.UTF_8.decode(request.getContent()).toString(), DraftRequest.class);
      } catch (JsonSyntaxException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Unable to decode request body: " + e.getMessage());
        return;
      } catch (IllegalArgumentException e) {
        responder.sendError(HttpURLConnection.HTTP_BAD_REQUEST, "Invalid config: " + e.getMessage());
        return;
      }

      long createTime = System.currentTimeMillis();
      if (drafts.containsKey(draftId)) {
        createTime = drafts.get(draftId).getCreatedTimeMillis();
      }

      drafts.put(draftId, new Draft(draft.getName(), draftId, draft.getConfig(), 0, draft.getType(), createTime, System
        .currentTimeMillis()));
//      draftService.saveDraft(new DraftId(namespace, draftId), draft);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }

  @DELETE
  @Path("v1/contexts/{context}/drafts/{draft}")
  public void deleteDraft(HttpServiceRequest request, HttpServiceResponder responder,
    @PathParam("context") String namespaceName,
    @PathParam("draft") String draftName) {
    respond(namespaceName, responder, (draftService, namespace) -> {
      if (!drafts.containsKey(draftName)) {
        throw new DraftNotFoundException(new DraftId(new NamespaceSummary(namespaceName, "", 1), draftName));
      }

      drafts.remove(draftName);
      responder.sendStatus(HttpURLConnection.HTTP_OK);
    });
  }


  /**
   * Utility method that checks that the namespace exists before responding.
   */
  private void respond(String namespaceName, HttpServiceResponder responder, NamespacedEndpoint endpoint) {
    SystemHttpServiceContext context = getContext();

    NamespaceSummary namespaceSummary;
    try {
      namespaceSummary = context.getAdmin().getNamespaceSummary(namespaceName);
      if (namespaceSummary == null) {
        responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, String.format("Namespace '%s' not found", namespaceName));
        return;
      }
    } catch (IOException e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR,
        String.format("Unable to check if namespace '%s' exists.", namespaceName));
      return;
    }

    try {
      endpoint.respond(null, namespaceSummary);
    } catch (CodedException e) {
      responder.sendError(e.getCode(), e.getMessage());
    } catch (TableNotFoundException e) {
      responder.sendError(HttpURLConnection.HTTP_NOT_FOUND, e.getMessage());
    } catch (Exception e) {
      responder.sendError(HttpURLConnection.HTTP_INTERNAL_ERROR, e.getMessage());
    }
  }

  /**
   * Encapsulates the core logic that needs to happen in an endpoint.
   */
  private interface NamespacedEndpoint {

    /**
     * Create the response that should be returned by the endpoint.
     */
    void respond(StudioService draftService, NamespaceSummary namespace) throws Exception;
  }

}
