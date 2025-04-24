package org.example.server;

import io.vertx.core.Vertx;
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

    private static String ID = "id";

    private static final Logger logger = LoggerFactory.getLogger(DBService.class);

    private static final JsonObject formattedRequestBody = new JsonObject();

    public DBService(Vertx vertx)
    {
        this.vertx = vertx;
    }

    public void create(JsonObject requestBody, RoutingContext context) throws InterruptedException
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_INSERT)
                .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context))
                .put(Constants.DATA, requestBody);

        var query = QueryBuilder.buildQuery(formattedRequestBody);

        executeQuery(query, context);
    }

    public void getById(String id, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_SELECT)
                            .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context))
                            .put(Constants.CONDITIONS , new JsonObject().put(ID, Integer.parseInt(id)));

        var query = QueryBuilder.buildQuery(formattedRequestBody);

        executeQuery(query, context);
    }

    public void getAll(RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_SELECT)
                            .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context));

        var query = QueryBuilder.buildQuery(formattedRequestBody);

        executeQuery(query, context);
    }

    public void update(String id,JsonObject requestBody, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_UPDATE)
                .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context))
                .put(Constants.DATA, requestBody)
                .put(Constants.CONDITIONS , new JsonObject().put(ID, Integer.parseInt(id)));

        var query = QueryBuilder.buildQuery(formattedRequestBody);

        executeQuery(query, context);
    }

    public void delete(String id, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_DELETE)
                .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context))
                .put(Constants.CONDITIONS , new JsonObject().put(ID, Integer.parseInt(id)));

        var query = QueryBuilder.buildQuery(formattedRequestBody);

        executeQuery(query, context);
    }

    public void addForProvision(String id, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_SELECT)
                .put(Constants.TABLE_NAME, "discovery_profiles")
                .put(Constants.CONDITIONS , new JsonObject().put(ID, Integer.parseInt(id)));

        var query = QueryBuilder.buildQuery(formattedRequestBody);

        vertx.eventBus().request(Constants.EVENTBUS_DATABASE_ADDRESS, query,reply ->
        {
                    if (reply.succeeded())
                    {
                        var result = (JsonObject) reply.result().body();

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
                                        .put("ip", discoveryProfile.getString("ip"))
                                        .put("port", discoveryProfile.getInteger("port"))
                                        .put("credential_profile_id", discoveryProfile.getInteger("credential_profile_id")));

                        var query2 = QueryBuilder.buildQuery(formattedRequestBody);

                        executeQuery(query2, context);
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
        vertx.eventBus().request(Constants.EVENTBUS_DATABASE_ADDRESS, query, ar -> {

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

        var placeholders = QueryBuilder.buildPlaceholders(ids.size());

        String fetchQuery = "SELECT dp.id, dp.ip, dp.port, cp.credentials FROM discovery_profiles dp " +
                "JOIN credential_profiles cp ON dp.credential_profile_id = cp.id " +
                "WHERE dp.id IN (" + placeholders + ")";

        JsonObject request = new JsonObject()
                .put("query", fetchQuery)
                .put("params", ids);

        vertx.eventBus().request(Constants.EVENTBUS_DATABASE_ADDRESS, request, dbRes ->
        {
            if (dbRes.failed())
            {
                ctx.response().setStatusCode(500).end("DB query failed");

                return;
            }

            var resBody = (JsonObject) dbRes.result().body();

            if (!resBody.getBoolean("success") || resBody.getJsonArray("data").isEmpty())
            {
                ctx.response().setStatusCode(404).end("No discovery data found");

                return;
            }

            var deviceData = resBody.getJsonArray("data");

            logger.info("Processing fping of : {}", deviceData);

            vertx.executeBlocking(promise -> {

                var responseArray = new JsonArray();

                for (int i = 0; i < deviceData.size(); i++)
                {
                    responseArray.add(new JsonObject()
                            .put("id", deviceData.getJsonObject(i).getInteger("id"))
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

                var sshOutput = Utils.runGoPlugin(aliveDevices, "discovery");

                if (sshOutput.isEmpty())
                {
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

                    resultMap.put(obj.getInteger("id"), obj);
                }

                var batchParams = new JsonArray();

                // Update the response array in-place
                for (int i = 0; i < responseArray.size(); i++)
                {
                    var response = responseArray.getJsonObject(i);

                    var id = response.getInteger("id");

                    if (resultMap.containsKey(id))
                    {
                        var pluginResult = resultMap.get(id);

                        boolean success = pluginResult.getBoolean("success");

                        String step = pluginResult.getString("step");

                        response.put("success", success);
                        response.put("reason", step);
                    }

                    batchParams.add(new JsonArray()
                            .add(response.getBoolean("success"))
                            .add(id));
                }

                // Prepare batch update
                var updateRequest = new JsonObject()
                        .put("query", "UPDATE discovery_profiles SET status = $1 WHERE id = $2")
                        .put("params", batchParams);

                // Send update to batch consumer
                vertx.eventBus().request(Constants.EVENTBUS_BATCH_UPDATE_ADDRESS, updateRequest, updateRes ->
                {
                    if (updateRes.failed())
                    {
                        logger.error("Batch update failed: {}", updateRes.cause().getMessage());

                        promise.complete(responseArray); // still return results
                    }
                    else
                    {
                        logger.info("Batch update completed");

                        promise.complete(responseArray);
                    }
                });

            }, false, result ->
            {
                if (result.failed())
                {
                    ctx.response().setStatusCode(500).end("Discovery failed");
                }
                else
                {
                    var responseArray = (JsonArray) result.result();

                    ctx.response()
                            .putHeader("Content-Type", "application/json")
                            .end(responseArray.encodePrettily());
                }
            });
        });
    }
}
