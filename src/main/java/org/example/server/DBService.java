package org.example.server;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.example.utils.Constants;
import org.example.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBService {

    private final Vertx vertx;

    private static String ID = "id";

    private static final Logger logger = LoggerFactory.getLogger(DBService.class);

    private static final JsonObject formattedRequestBody = new JsonObject();

    public DBService(Vertx vertx)
    {
        this.vertx = vertx;
    }

    public void create(JsonObject requestBody, RoutingContext context)
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

    public void runDiscovery(String id, RoutingContext context) {
        formattedRequestBody.clear();

        var query = """
            SELECT
              d.id AS discovery_id,
              d.discovery_profile_name,
              d.ip,
              d.port,
              c.credentials
            FROM
              discovery_profiles d
            JOIN
              credential_profiles c
            ON
              d.credential_profile_id = c.id
            WHERE
              d.id = $1;
        """;

        var queryObj = new JsonObject()
                .put("query", query)
                .put("params", new JsonArray().add(Integer.parseInt(id)));

        vertx.eventBus().request(Constants.EVENTBUS_DATABASE_ADDRESS, queryObj, reply -> {
            if (reply.failed()) {
                context.response().setStatusCode(500).end("DB error: " + reply.cause().getMessage());
                return;
            }

            var result = (JsonObject) reply.result().body();
            if (!result.getBoolean(Constants.SUCCESS)) {
                context.response().setStatusCode(500).end("DB query failed: " + result.getString(Constants.ERROR));
                return;
            }

            var data = result.getJsonArray(Constants.DATA);
            if (data.isEmpty()) {
                context.response().setStatusCode(404).end("Discovery ID not found.");
                return;
            }

            var device = data.getJsonObject(0);
            var ip = device.getString("ip");
            var port = device.getInteger("port");
            var credentials = device.getJsonObject("credentials");
            var discoveryId = device.getInteger("discovery_id");

            // Start discovery in worker thread
            vertx.executeBlocking(promise -> {
                var success = Utils.runPing(ip);
                if (!success) {
                    promise.complete(new JsonObject().put("success", false).put("step", "ping"));
                    return;
                }

                success = Utils.runPortCheck(ip, port);
                if (!success) {
                    promise.complete(new JsonObject().put("success", false).put("step", "port"));
                    return;
                }

//                var sshSuccess = runSSHPluginTest(ip, port, credentials);
//                if (!sshSuccess) {
//                    promise.complete(new JsonObject().put("success", false).put("step", "ssh"));
//                    return;
//                }

                promise.complete(new JsonObject().put("success", true));
            }, res -> {
                var resultObj = (JsonObject) res.result();
                var success = resultObj.getBoolean("success");
                var step = resultObj.getString("step", "all");

                // Save result to DB
                var updateQuery = """
                UPDATE discovery_profiles
                SET status = $1
                WHERE id = $2
                RETURNING id;
            """;

                var status = success ? "true" : "false";

                var updateObj = new JsonObject()
                        .put("query", updateQuery)
                        .put("params", new JsonArray().add(Boolean.parseBoolean(status)).add(discoveryId));

                vertx.eventBus().request(Constants.EVENTBUS_DATABASE_ADDRESS, updateObj, updateReply -> {
                    if (updateReply.failed()) {
                        context.response().setStatusCode(500).end("Failed to update discovery status");
                        return;
                    }

                    if (success) {
                        context.response().end("✅ Discovery successful");
                    } else {
                        context.response().end("❌ Discovery failed at step: " + step);
                    }
                });
            });
        });
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


}
