package org.example.polling;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.example.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PollingSchedulerVerticle extends AbstractVerticle
{
    private static final Logger logger = LoggerFactory.getLogger(PollingSchedulerVerticle.class);

    private static final int BATCH_SIZE = 2;

    private static final String QUERY_FETCH_JOBS = """
            SELECT pj.id AS id, pj.ip, pj.port, cp.credentials
            FROM provisioning_jobs pj
            JOIN credential_profiles cp ON pj.credential_profile_id = cp.id
            ORDER BY pj.id
            LIMIT $1 OFFSET $2
        """;

    @Override
    public void start(Promise<Void> startPromise)
    {
        vertx.setPeriodic(60_000, 300_000, id -> runPollingScheduler());

        startPromise.complete();
    }

    private void runPollingScheduler()
    {
        logger.info("Polling cycle started");

        fetchBatch(0);
    }

    private void fetchBatch(int offset)
    {
        var params = new JsonArray().add(BATCH_SIZE).add(offset);

        var request = new JsonObject()
                .put(Constants.QUERY, QUERY_FETCH_JOBS)
                .put(Constants.PARAMS, params);

        vertx.eventBus().request(Constants.EVENTBUS_DATABASE_ADDRESS, request, reply ->
        {
            if (reply.succeeded())
            {
                var response = (JsonObject) reply.result().body();

                if (response.getBoolean(Constants.SUCCESS))
                {
                    var data = response.getJsonArray(Constants.DATA);

                    if (!data.isEmpty())
                    {
                        logger.info("Sending batch of size {}", data.size() + " to PollingProcessor");

                        logger.info(data.toString());

                        vertx.eventBus().send(Constants.EVENTBUS_POLLING_PROCESSOR_ADDRESS, data);

                        fetchBatch(offset + BATCH_SIZE);
                    }
                    else
                    {
                        logger.info("All provisioning jobs are processed");
                    }
                }
                else
                {
                    logger.error("DB query failed: {}" , response.getString(Constants.ERROR));
                }
            }
            else
            {
                logger.error("DB call failed: {}" ,reply.cause().getMessage());
            }
        });
    }
}

