package org.example.service;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.example.utils.Constants;
import org.example.utils.Utils;

public class QueryBuilderVerticle extends AbstractVerticle
{
    @Override
    public void start(Promise<Void> promise)
    {
        setUpeventBusConsumer();

        promise.complete();
    }

    private void setUpeventBusConsumer()
    {
        vertx.eventBus().consumer(Constants.EVENTBUS_QUERYBUILDER_ADDRESS, message ->
        {
            vertx.executeBlocking(promise -> {

                var input = (JsonObject) message.body();

                var operation = input.getString(Constants.OPERATION);

                var table = input.getString(Constants.TABLE_NAME);

                var data = input.getJsonObject(Constants.DATA, new JsonObject());

                var conditions = input.getJsonObject(Constants.CONDITIONS, new JsonObject());

                var columns = input.getJsonArray(Constants.COLUMNS, new JsonArray());

                var params = new JsonArray();

                var query = new StringBuilder();

                switch (operation.toLowerCase())
                {
                    case Constants.DB_INSERT:

                        var keys = data.fieldNames();

                        var columnsStr = String.join(", ", keys);

                        var placeholders = Utils.buildPlaceholders(keys.size());

                        for (var key : keys)
                        {
                            params.add(data.getValue(key));
                        }

                        query.append("INSERT INTO ").append(table)
                                .append(" (").append(columnsStr).append(")")
                                .append(" VALUES (").append(placeholders).append(")");

                        break;

                    case Constants.DB_SELECT:

                        var columnStr = (columns != null && !columns.isEmpty())
                                ? String.join(", ", columns.stream().map(Object::toString).toList())
                                : "*";

                        query.append("SELECT ").append(columnStr).append(" FROM ").append(table);

                        if (!conditions.isEmpty())
                        {
                            var whereClause = buildWhereClause(conditions, params, 1);

                            query.append(" ").append(whereClause);
                        }
                        break;

                    case Constants.DB_UPDATE:

                        query.append("UPDATE ").append(table).append(" SET ");

                        var index = 1;

                        for (var key : data.fieldNames())
                        {
                            query.append(key).append(" = $").append(index++);

                            if (index <= data.size()) query.append(", ");

                            params.add(data.getValue(key));
                        }

                        if (!conditions.isEmpty())
                        {
                            var whereClause = buildWhereClause(conditions, params, index);

                            query.append(" ").append(whereClause);
                        }
                        break;

                    case Constants.DB_DELETE:

                        query.append("DELETE FROM ").append(table);

                        if (!conditions.isEmpty())
                        {
                            var whereClause = buildWhereClause(conditions, params, 1);

                            query.append(" ").append(whereClause);
                        }
                        break;

                    default:

                        promise.fail("Invalid operation: " + operation);

                        return;
                }

                promise.complete(new JsonObject().put(Constants.QUERY, query.toString()).put(Constants.PARAMS, params));
            }, false , result ->
            {
                if (result.succeeded())
                {
                    message.reply(result.result());
                }
                else
                {
                    message.fail(500, result.cause().getMessage());
                }
            });
        });
    }

    private String buildWhereClause(JsonObject conditions, JsonArray params, int paramStartIndex)
    {
        var clause = new StringBuilder("WHERE ");

        int i = 0;

        for (var key : conditions.fieldNames())
        {
            if (i > 0) clause.append(" AND ");

            clause.append(key).append(" = $").append(paramStartIndex + i);

            params.add(conditions.getValue(key));

            i++;
        }
        return clause.toString();
    }

    @Override
    public void stop() {

    }
}
