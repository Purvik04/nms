package org.example.server;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import static org.example.utils.Constants.*;

public class QueryBuilder
{
    public static JsonObject buildQuery(JsonObject input)
    {
        var operation = input.getString(OPERATION);

        var table = input.getString(TABLE_NAME);

        var data = input.getJsonObject(DATA, new JsonObject());

        var conditions = input.getJsonObject(CONDITIONS, new JsonObject());

        var columns = input.getJsonArray(COLUMNS, new JsonArray());

        var idColumn = input.getString("idColumn", "id");

        var idValue = input.getString("idValue");

        var params = new JsonArray();

        var query = new StringBuilder();

        switch (operation.toLowerCase())
        {
            case DB_INSERT:

                var keys = data.fieldNames();

                var columnsStr = String.join(", ", keys);

                var placeholders = buildPlaceholders(keys.size());

                for (var key : keys)
                {
                    params.add(data.getValue(key));
                }

                query.append("INSERT INTO ").append(table)
                        .append(" (").append(columnsStr).append(")")
                        .append(" VALUES (").append(placeholders).append(")");

                break;

            case DB_SELECT:

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

            case DB_UPDATE:

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

            case DB_DELETE:

                query.append("DELETE FROM ").append(table);

                if (!conditions.isEmpty())
                {
                    var whereClause = buildWhereClause(conditions, params, 1);

                    query.append(" ").append(whereClause);
                }
                break;

            default:

                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }

        return new JsonObject().put("query", query.toString()).put("params", params);
    }

    private static String buildPlaceholders(int count)
    {
        var sb = new StringBuilder();

        for (int i = 1; i <= count; i++)
        {
            sb.append("$").append(i);

            if (i < count) sb.append(", ");
        }

        return sb.toString();
    }

    private static String buildWhereClause(JsonObject conditions, JsonArray params, int paramStartIndex)
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
}


