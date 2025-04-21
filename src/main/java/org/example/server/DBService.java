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
                            .put(Constants.CONDITIONS , new JsonObject().put("id", Integer.parseInt(id)));

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
                .put(Constants.CONDITIONS , new JsonObject().put("id", Integer.parseInt(id)));

        var query = QueryBuilder.buildQuery(formattedRequestBody);

        executeQuery(query, context);
    }

    public void delete(String id, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_DELETE)
                .put(Constants.TABLE_NAME, Utils.getTableNameFromContext(context))
                .put(Constants.CONDITIONS , new JsonObject().put("id", Integer.parseInt(id)));

        var query = QueryBuilder.buildQuery(formattedRequestBody);

        executeQuery(query, context);
    }

    public void runDiscovery(String id, RoutingContext context)
    {
        formattedRequestBody.clear();

        var query = """
                SELECT
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

        executeQuery(new JsonObject().put("query" , query).put("params", new JsonArray().add(Integer.parseInt(id))), context);
    }

    public void addForProvision(String id, RoutingContext context)
    {
        formattedRequestBody.clear();

        formattedRequestBody.put(Constants.OPERATION, Constants.DB_SELECT)
                .put(Constants.TABLE_NAME, "discovery_profiles")
                .put(Constants.CONDITIONS , new JsonObject().put("id", Integer.parseInt(id)));

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
