package org.example.service;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.example.utils.Constants;
import org.example.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class DBService
{
    private final Vertx vertx;

    private final EventBus eventBus;

    private static final Logger logger = LoggerFactory.getLogger(DBService.class);

    private static final JsonObject formattedRequestBody = new JsonObject();

    public DBService(Vertx vertx)
    {
        this.vertx = vertx;

        this.eventBus = vertx.eventBus();
    }

    public void create(JsonObject requestBody, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_INSERT)
                .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context))
                .put(Constants.DATA, requestBody);

       sendToQueryBuilder(formattedRequestBody, context);
    }

    public void getById(String id, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_SELECT)
                            .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context))
                            .put(Constants.CONDITIONS , new JsonObject().put(Constants.ID, Integer.parseInt(id)));

        sendToQueryBuilder(formattedRequestBody, context);
    }

    public void getAll(RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_SELECT)
                            .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context));

        sendToQueryBuilder(formattedRequestBody, context);
    }

    public void update(String id,JsonObject requestBody, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_UPDATE)
                .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context))
                .put(Constants.DATA, requestBody)
                .put(Constants.CONDITIONS , new JsonObject().put(Constants.ID, Integer.parseInt(id)));

        sendToQueryBuilder(formattedRequestBody, context);
    }

    public void delete(String id, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_DELETE)
                .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context))
                .put(Constants.CONDITIONS , new JsonObject().put(Constants.ID, Integer.parseInt(id)));

        sendToQueryBuilder(formattedRequestBody, context);
    }

    public void addForProvision(String id, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_SELECT)
                .put(Constants.TABLE_NAME, "discovery_profiles")
                .put(Constants.CONDITIONS , new JsonObject().put(Constants.ID, Integer.parseInt(id)));

        // Send the request to the query builder
        eventBus.request(Constants.EVENTBUS_QUERYBUILDER_ADDRESS, formattedRequestBody,reply ->
        {
            if(reply.succeeded())
            {
                //send query to database for execution
                eventBus.request(Constants.EVENTBUS_DATABASE_ADDRESS, reply.result().body(),dbReply ->
                {
                    if (dbReply.succeeded())
                    {
                        var result = (JsonObject) dbReply.result().body();

                        if (!result.getBoolean(Constants.SUCCESS, false))
                        {
                            context.response()
                                    .setStatusCode(500)
                                    .end(new JsonObject().put(Constants.ERROR, result.getString(Constants.ERROR)).encodePrettily());

                            return;
                        }

                        var rows = result.getJsonArray(Constants.DATA);

                        if (rows == null || rows.isEmpty())
                        {
                            context.response()
                                    .setStatusCode(404)
                                    .end(new JsonObject().put(Constants.ERROR, "Discovery profile not found").encodePrettily());

                            return;
                        }

                        var discoveryProfile = rows.getJsonObject(0);

                        var isDiscovered = discoveryProfile.getBoolean("status", false);

                        if (!isDiscovered)
                        {
                            context.response()
                                    .setStatusCode(400)
                                    .end(new JsonObject().put(Constants.ERROR, "Device is not discovered yet").encodePrettily());

                            return;
                        }

                        formattedRequestBody.clear();

                        formattedRequestBody.put(Constants.OPERATION, Constants.DB_INSERT)
                                .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context))
                                .put(Constants.DATA, new JsonObject()
                                        .put(Constants.IP, discoveryProfile.getString(Constants.IP))
                                        .put(Constants.PORT, discoveryProfile.getInteger(Constants.PORT))
                                        .put(Constants.CREDENTIAL_PROFILE_ID, discoveryProfile.getInteger(Constants.CREDENTIAL_PROFILE_ID)));

                        sendToQueryBuilder(formattedRequestBody, context);
                    }
                    else
                    {
                        context.response()
                                .setStatusCode(500)
                                .end(new JsonObject().put(Constants.ERROR, "DBService failed: " + reply.cause().getMessage()).encodePrettily());
                    }
                });
            }
            else
            {
                context.response()
                        .setStatusCode(500)
                        .end(new JsonObject().put(Constants.ERROR, "DBService failed: " + reply.cause().getMessage()).encodePrettily());

            }
        });
    }

    public void sendToQueryBuilder(JsonObject formattedRequest, RoutingContext context)
    {
        eventBus.request(Constants.EVENTBUS_QUERYBUILDER_ADDRESS, formattedRequest,reply ->
        {
            if(reply.succeeded())
            {
                executeQuery((JsonObject) reply.result().body() , context);
            }
            else
            {
                context.response()
                        .setStatusCode(500)
                        .end(new JsonObject().put(Constants.ERROR, "DBService failed: " + reply.cause().getMessage()).encodePrettily());

            }
        });
    }
    public void executeQuery(JsonObject query, RoutingContext context)
    {
        eventBus.request(Constants.EVENTBUS_DATABASE_ADDRESS, query, ar -> {

            if (ar.succeeded())
            {
                var result = (JsonObject) ar.result().body();

                if (result.getBoolean(Constants.SUCCESS, false))
                {
                    context.response()
                            .setStatusCode(201)
                            .end(result.encodePrettily());
                }
                else
                {
                    context.response()
                            .setStatusCode(500)
                            .end(new JsonObject()
                                    .put(Constants.ERROR, result.getString(Constants.ERROR))
                                    .encodePrettily());
                }
            }
            else
            {
                context.response()
                        .setStatusCode(500)
                        .end(new JsonObject()
                                .put(Constants.ERROR,"DBService failed: " + ar.cause().getMessage())
                                .encodePrettily());
            }
        });
    }

    public void runDiscovery(JsonArray ids, RoutingContext ctx) {

        var placeholders = Utils.buildPlaceholders(ids.size());

        var fetchQuery = "SELECT dp.id, dp.ip, dp.port, cp.credentials FROM discovery_profiles dp " +
                "JOIN credential_profiles cp ON dp.credential_profile_id = cp.id " +
                "WHERE dp.id IN (" + placeholders + ")";

        var request = new JsonObject()
                .put(Constants.QUERY, fetchQuery)
                .put(Constants.PARAMS, ids);

        eventBus.request(Constants.EVENTBUS_DATABASE_ADDRESS, request, dbRes ->
        {
            if (dbRes.failed())
            {
                ctx.response().setStatusCode(500).end("DB query failed");

                return;
            }

            vertx.executeBlocking(promise ->
            {
                var resBody = (JsonObject) dbRes.result().body();

                if (!resBody.getBoolean("success") || resBody.getJsonArray(Constants.DATA).isEmpty())
                {
                    promise.fail("No discovery data found");

                    return;
                }

                var deviceData = resBody.getJsonArray(Constants.DATA);

                logger.info("Processing fping of : {}", deviceData);

                var responseArray = new JsonArray();

                for (int i = 0; i < deviceData.size(); i++)
                {
                    responseArray.add(new JsonObject()
                            .put(Constants.ID, deviceData.getJsonObject(i).getInteger(Constants.ID))
                            .put("success", false)
                            .put("reason", "ping failed"));
                }

                var aliveDevices = Utils.runFping(deviceData);

                if (aliveDevices.isEmpty())
                {
                    promise.complete(responseArray);

                    return;
                }

                logger.info("Processing SSH discovery of : {}", aliveDevices);

                var sshOutput = Utils.runGoPlugin(aliveDevices, Constants.DISCOVERY);

                if (sshOutput.isEmpty())
                {
                    logger.error("Go plugin execution failed");

                    promise.complete(responseArray);

                    return;
                }

                logger.info("Discovery Process Completed");

                var sshDiscoveredDevices = new JsonArray(sshOutput);

                logger.info(sshDiscoveredDevices.toString());

                // Create map of id -> result from plugin
                var resultMap = new HashMap<Integer, JsonObject>();

                for (int i = 0; i < sshDiscoveredDevices.size(); i++)
                {
                    var obj = sshDiscoveredDevices.getJsonObject(i);

                    resultMap.put(obj.getInteger(Constants.ID), obj);
                }

                var batchParams = new JsonArray();

                // Update the response array in-place
                for (int i = 0; i < responseArray.size(); i++)
                {
                    var response = responseArray.getJsonObject(i);

                    var id = response.getInteger(Constants.ID);

                    if (resultMap.containsKey(id))
                    {
                        var pluginResult = resultMap.get(id);

                        boolean success = pluginResult.getBoolean("success");

                        var step = pluginResult.getString("step");

                        response.put("success", success);
                        response.put("reason", step);
                    }

                    batchParams.add(new JsonArray()
                            .add(response.getBoolean("success"))
                            .add(id));
                }

                // Prepare batch update
                var updateRequest = new JsonObject()
                        .put(Constants.QUERY, "UPDATE discovery_profiles SET status = $1 WHERE id = $2")
                        .put(Constants.PARAMS, batchParams);

                // Send update to batch consumer
                eventBus.request(Constants.EVENTBUS_DATABASE_ADDRESS, updateRequest, updateRes ->
                {
                    var jsonResponse = (JsonObject) updateRes.result().body();

                    if (jsonResponse.getBoolean("success", false))
                    {
                        promise.complete(responseArray);
                    }
                    else
                    {
                        promise.fail(jsonResponse.getString("error"));
                    }
                });

            }, false, result ->
            {
                if (result.failed())
                {
                    ctx.response().setStatusCode(404).end(result.cause().getMessage());
                }
                else
                {
                    ctx.json(result.result());
                }
            });
        });
    }
}
