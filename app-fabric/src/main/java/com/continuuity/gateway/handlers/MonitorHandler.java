package com.continuuity.gateway.handlers;

import com.continuuity.common.conf.Constants;
import com.continuuity.common.discovery.EndpointStrategy;
import com.continuuity.common.discovery.RandomEndpointStrategy;
import com.continuuity.common.discovery.TimeLimitEndpointStrategy;
import com.continuuity.common.twill.ReactorServiceManager;
import com.continuuity.gateway.auth.Authenticator;
import com.continuuity.gateway.handlers.util.AbstractAppFabricHttpHandler;
import com.continuuity.http.HttpResponder;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMultimap;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.ning.http.client.Response;
import com.ning.http.client.SimpleAsyncHttpClient;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 * Monitor Handler returns the status of different discoverable services
 */
@Path(Constants.Gateway.GATEWAY_VERSION)
public class MonitorHandler extends AbstractAppFabricHttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(MonitorHandler.class);
  private final DiscoveryServiceClient discoveryServiceClient;
  private final Map<String, ReactorServiceManager> reactorServiceManagementMap;
  private static final String STATUS_OK = "OK";
  private static final String STATUS_NOTOK = "NOTOK";

  /**
   * Timeout to get response from discovered service.
   */
  private static final long SERVICE_PING_RESPONSE_TIMEOUT = TimeUnit.MILLISECONDS.convert(1, TimeUnit.MINUTES);

  /**
   * Number of seconds for timing out a service endpoint discovery.
   */
  private static final long DISCOVERY_TIMEOUT_SECONDS = 3;

  private enum Service {
    METRICS (Constants.Service.METRICS),
    TRANSACTION (Constants.Service.TRANSACTION),
    STREAMS (Constants.Service.STREAMS),
    APPFABRIC (Constants.Service.APP_FABRIC_HTTP);

    private final String name;

    private Service(String name) {
      this.name = name;
    }

    public String getName() { return name; }

    public static Service valueofName(String name) { return valueOf(name.toUpperCase()); }
  }

  //List of services whose status can be retrieved
  private List<Service> monitorList = Arrays.asList(Service.METRICS, Service.TRANSACTION, Service.STREAMS,
                                                    Service.APPFABRIC);

  @Inject
  public MonitorHandler(Authenticator authenticator, DiscoveryServiceClient discoveryServiceClient,
                        Map<String, ReactorServiceManager> serviceMap) {
    super(authenticator);
    this.discoveryServiceClient = discoveryServiceClient;
    this.reactorServiceManagementMap = serviceMap;
  }

  /**
   * Stops Reactor Service
   */
  @Path("/system/services/{service-name}/stop")
  @POST
  public void stopService(final HttpRequest request, final HttpResponder responder,
                          @PathParam("service-name") String serviceName) {
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

  /**
   * Starts Reactor Service
   */
  @Path("/system/services/{service-name}/start")
  @POST
  public void startService(final HttpRequest request, final HttpResponder responder,
                           @PathParam("service-name") String serviceName) {
    responder.sendStatus(HttpResponseStatus.NOT_IMPLEMENTED);
  }

  /**
   * Returns the number of instances of Reactor Services
   */
  @Path("/system/services/{service-name}/instances")
  @GET
  public void getServiceInstance(final HttpRequest request, final HttpResponder responder,
                                 @PathParam("service-name") String serviceName) {
    if (reactorServiceManagementMap.containsKey(serviceName)) {
      int instances = reactorServiceManagementMap.get(serviceName).getInstances();
      responder.sendString(HttpResponseStatus.OK, String.valueOf(instances));
    } else {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "Invalid Service Name or Operation not valid for this service");
    }
  }

  /**
   * Sets the number of instances of Reactor Services
   */
  @Path("/system/services/{service-name}/instances")
  @PUT
  public void setServiceInstance(final HttpRequest request, final HttpResponder responder,
                                 @PathParam("service-name") String serviceName) {
    try {
      int instance = getInstances(request);
      if (instance < 1) {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Instance count should be greater than 0");
        return;
      }

      if (reactorServiceManagementMap.get(serviceName).setInstances(instance)) {
        responder.sendStatus(HttpResponseStatus.OK);
      } else {
        responder.sendString(HttpResponseStatus.BAD_REQUEST, "Operation Not Valid for this service");
      }
    } catch (Exception e) {
      responder.sendString(HttpResponseStatus.BAD_REQUEST,
                           "Invalid Service Name Or Operation Not Valid for this service");
    }
  }

  //Return the status of reactor services in JSON format
  @Path("/system/services/status")
  @GET
  public void getBootStatus(final HttpRequest request, final HttpResponder responder) {
    Map<String, String> result = new HashMap<String, String>();
    String json;
    for (Service service : monitorList) {
      String serviceName = String.valueOf(service);
      String status = discoverService(serviceName) ? STATUS_OK : STATUS_NOTOK;
      result.put(serviceName, status);
    }

    json = (new Gson()).toJson(result);
    responder.sendByteArray(HttpResponseStatus.OK, json.getBytes(Charsets.UTF_8),
                            ImmutableMultimap.of(HttpHeaders.Names.CONTENT_TYPE, "application/json"));
  }

  @Path("/system/services/{service-id}/status")
  @GET
  public void monitor(final HttpRequest request, final HttpResponder responder,
                      @PathParam("service-id") final String service) {
    if (discoverService(service)) {
      //Service is discoverable
      String response = String.format("%s is OK\n", service);
      responder.sendString(HttpResponseStatus.OK, response);
    } else {
      String response = String.format("%s not found\n", service);
      responder.sendString(HttpResponseStatus.NOT_FOUND, service);
    }
  }

  private boolean discoverService(String serviceName) {
    try {
      //TODO: Return true until we make txService health check work in both SingleNode and DistributedMode
      if (Service.valueofName(serviceName).equals(Service.TRANSACTION)) {
        return true;
      }

      Iterable<Discoverable> discoverables = this.discoveryServiceClient.discover(Service.valueofName(
        serviceName).getName());
      EndpointStrategy endpointStrategy = new TimeLimitEndpointStrategy(
        new RandomEndpointStrategy(discoverables), DISCOVERY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
      Discoverable discoverable = endpointStrategy.pick();
      //Transaction Service will return null discoverable in SingleNode mode
      if (discoverable == null) {
        return false;
      }

      //Ping the discovered service to check its status.
      String url = String.format("http://%s:%d/ping", discoverable.getSocketAddress().getHostName(),
                                 discoverable.getSocketAddress().getPort());
      return checkGetStatus(url).equals(HttpResponseStatus.OK);
    } catch (IllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      LOG.warn("Unable to ping {} : Reason : {}", serviceName, e.getMessage());
      return false;
    }
  }

  private HttpResponseStatus checkGetStatus(String url) throws Exception {
    SimpleAsyncHttpClient client = new SimpleAsyncHttpClient.Builder()
      .setUrl(url)
      .setRequestTimeoutInMs((int) SERVICE_PING_RESPONSE_TIMEOUT)
      .build();

    try {
      Future<Response> future = client.get();
      Response response = future.get(SERVICE_PING_RESPONSE_TIMEOUT, TimeUnit.MILLISECONDS);
      return HttpResponseStatus.valueOf(response.getStatusCode());
    } catch (Exception e) {
      Throwables.propagate(e);
    } finally {
      client.close();
    }
    return HttpResponseStatus.NOT_FOUND;
  }
}
