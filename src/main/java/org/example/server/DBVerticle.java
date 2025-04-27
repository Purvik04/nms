package org.example.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Tuple;
import org.example.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DBVerticle extends AbstractVerticle
{
    private static final Logger logger = LoggerFactory.getLogger(DBVerticle.class);

    private static final String RETURNING_ID = " RETURNING id";

    private Pool client;

    @Override
    public void start(Promise<Void> startPromise)
    {
        var connectOptions = new PgConnectOptions()
                .setHost(Constants.DB_HOST)
                .setPort(Integer.parseInt(Constants.DB_PORT))
                .setDatabase(Constants.DB_NAME)
                .setUser(Constants.DB_USER)
                .setPassword(Constants.DB_PASSWORD);

        var poolOptions = new PoolOptions()
                .setMaxSize(10);

        client = PgBuilder.pool()
                .with(poolOptions)
                .connectingTo(connectOptions)
                .using(vertx)
                .build();

        client.query(Constants.DB_CONNECTION_CHECKUP_QUERY).execute(ar ->
        {
            if (ar.succeeded())
            {
                logger.info("✅ Connected to PostgreSQL successfully!");

                createTables(startPromise);
            }
            else
            {
                logger.info("❌ DB connection failed: {}" , ar.cause().getMessage());

                startPromise.fail(ar.cause());
            }
        });
    }

    private void createTables(Promise<Void> startPromise)
    {
        var credentialProfilesTable = """
            CREATE TABLE IF NOT EXISTS credential_profiles (
                id SERIAL PRIMARY KEY,
                credential_profile_name TEXT UNIQUE NOT NULL,
                system_type TEXT NOT NULL,
                credentials JSONB NOT NULL
            );
            """;

        var discoveryProfilesTable = """
            CREATE TABLE IF NOT EXISTS discovery_profiles (
                id SERIAL PRIMARY KEY,
                discovery_profile_name TEXT UNIQUE NOT NULL,
                credential_profile_id INT,
                ip TEXT NOT NULL,
                port INT NOT NULL,
                status BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (credential_profile_id) REFERENCES credential_profiles(id) ON DELETE RESTRICT
            );
            """;

        var provisioningJobsTable = """
           CREATE TABLE IF NOT EXISTS provisioning_jobs (
                id SERIAL PRIMARY KEY,
                credential_profile_id INT,
                ip TEXT NOT NULL UNIQUE,
                port INT NOT NULL,
                FOREIGN KEY (credential_profile_id) REFERENCES credential_profiles(id) ON DELETE RESTRICT
            );
           """;

        var provisionedDataTable = """
            CREATE TABLE IF NOT EXISTS provisioned_data (
                id SERIAL PRIMARY KEY,
                job_id INT NOT NULL REFERENCES provisioning_jobs(id) ON DELETE CASCADE,
                data JSONB NOT NULL,
                polled_at TEXT NOT NULL
            );
            """;

        var usersTable= """
           CREATE TABLE IF NOT EXISTS users (
                id SERIAL PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password TEXT NOT NULL
           );
           """;

        var queries = new String[] {
                credentialProfilesTable,
                discoveryProfilesTable,
                provisioningJobsTable,
                provisionedDataTable,
                usersTable
        };

        executeBatch(queries, 0, startPromise);
    }

    private void executeBatch(String[] queries, int index, Promise<Void> promise)
    {
        if (index >= queries.length)
        {
            logger.info("All tables are verified/created.");

            setupEventBusConsumer();

            promise.complete();

            return;
        }

        client.query(queries[index]).execute(ar ->
        {
            if (ar.succeeded())
            {
                executeBatch(queries, index + 1, promise);
            }
            else
            {
                logger.error("Table creation failed: {}", ar.cause().getMessage());

                promise.fail(ar.cause());
            }
        });
    }

    private void setupEventBusConsumer()
    {
        vertx.eventBus().consumer(Constants.EVENTBUS_DATABASE_ADDRESS, message -> {

            var input = (JsonObject) message.body();

            try
            {
                var query = input.getString(Constants.QUERY);

                if ((query.trim().toLowerCase().startsWith(Constants.DB_INSERT) || query.trim().toLowerCase().startsWith(Constants.DB_UPDATE)) && !query.toLowerCase().contains("returning")) {
                    query += RETURNING_ID;
                }

                var paramArray = input.getJsonArray(Constants.PARAMS, new JsonArray());

                if (!paramArray.isEmpty() && paramArray.getValue(0) instanceof JsonArray)
                {
                    var batchParams = new ArrayList<Tuple>();

                    for (int i = 0; i < paramArray.size(); i++)
                    {
                        var inner = paramArray.getJsonArray(i);

                        var tuple = Tuple.tuple();

                        for (int j = 0; j < inner.size(); j++) {
                            tuple.addValue(inner.getValue(j));
                        }
                        batchParams.add(tuple);
                    }

                    logger.info("Executing Batch Query: {} with {} parameter sets", query, batchParams.size());

                    client.preparedQuery(query).executeBatch(batchParams, ar ->
                    {
                        if (ar.succeeded())
                        {
                            logger.info("Batch query executed successfully");

                            message.reply(new JsonObject()
                                    .put(Constants.SUCCESS, true)
                                    .put(Constants.DATA, "Batch update successful"));
                        }
                        else
                        {
                            logger.error("❌ Batch query failed: {}", ar.cause().getMessage());

                            message.reply(new JsonObject()
                                    .put(Constants.SUCCESS, false)
                                    .put(Constants.ERROR, ar.cause().getMessage()));
                        }
                    });
                }
                else
                {
                    var tuple = Tuple.tuple();

                    // Handle single param which is a List (e.g., List<Integer> for ANY($1))
                    if (paramArray.size() == 1 && paramArray.getValue(0) instanceof List)
                    {
                        tuple.addArrayOfInteger((Integer[]) paramArray.getValue(0)); // directly add the List
                    }
                    else
                    {
                        for (int i = 0; i < paramArray.size(); i++)
                        {
                            tuple.addValue(paramArray.getValue(i));
                        }
                    }

                    logger.error(tuple.toString());

                    logger.info("Executing Query: {}", query);

                    client.preparedQuery(query).execute(tuple, ar ->
                    {
                        if (ar.succeeded())
                        {
                            var rows = ar.result();

                            var jsonRows = new JsonArray();

                            rows.forEach(row ->
                            {
                                var obj = new JsonObject();

                                for (int i = 0; i < row.size(); i++)
                                {
                                    obj.put(row.getColumnName(i), row.getValue(i));
                                }
                                jsonRows.add(obj);
                            });

                            message.reply(new JsonObject()
                                    .put(Constants.SUCCESS, true)
                                    .put(Constants.DATA, jsonRows));
                        }
                        else
                        {
                            logger.error("❌ Query failed: {}", ar.cause().getMessage());

                            message.reply(new JsonObject()
                                    .put(Constants.SUCCESS, false)
                                    .put(Constants.ERROR, ar.cause().getMessage()));
                        }
                    });
                }
            }
            catch (Exception exception)
            {
                message.reply(new JsonObject()
                        .put(Constants.SUCCESS, false)
                        .put(Constants.ERROR, exception.getMessage()));
            }
        });
    }

    @Override
    public void stop() {
        if (client != null) {
            client.close();
        }
    }
}
