package org.example.polling;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.example.utils.Constants;
import org.example.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PollingProcessorVerticle extends AbstractVerticle
{
    private static final Logger logger = LoggerFactory.getLogger(PollingProcessorVerticle.class);

    private static final String QUERY_INSERT_PROVISIONED_DATA = """
            INSERT INTO provisioned_data (job_id, data, polled_at)
            VALUES ($1, $2::jsonb, $3)
        """;

    @Override
    public void start(Promise<Void> startPromise)
    {
        vertx.eventBus().consumer(Constants.EVENTBUS_POLLING_PROCESSOR_ADDRESS, this::handlePollingBatch);

        startPromise.complete();
    }

    private void handlePollingBatch(Message<JsonArray> message)
    {
        var deviceBatch = message.body();

        logger.info("Received device batch of size: {}", deviceBatch.size());

        vertx.executeBlocking(promise ->
        {
            var pluginOutput = Utils.runGoPlugin(deviceBatch, Constants.METRICS);

            if (pluginOutput.isEmpty())
            {
                promise.fail("Go plugin execution failed");
                
                return;
            }

            var processedData = new JsonArray(pluginOutput);

            logger.info("Plugin processed batch, sending {} entries to DB", processedData.size());

            sendToDatabase(processedData);

            promise.complete();

        },false, res ->
        {
            if (res.failed())
            {
                logger.error(res.cause().getMessage());
            }
        });
    }

    private void sendToDatabase(JsonArray polledResults)
    {
        var batchParams = new JsonArray();

        for (int i = 0; i < polledResults.size(); i++){

            var deviceResult = polledResults.getJsonObject(i);

            batchParams.add(new JsonArray()
                    .add(deviceResult.getInteger(Constants.ID))
                    .add(deviceResult.getJsonObject(Constants.DATA))
                    .add(deviceResult.getString(Constants.POLLED_AT)));
        }

        var request = new JsonObject()
                .put(Constants.QUERY, QUERY_INSERT_PROVISIONED_DATA)
                .put(Constants.PARAMS, batchParams);

        vertx.eventBus().request(Constants.EVENTBUS_DATABASE_ADDRESS, request, reply ->
        {
            if (reply.failed())
            {
                logger.error("Batch update of provision failed: {}", reply.cause().getMessage());
            }
            else
            {
                logger.info("Batch update of provision completed");
            }
        });
    }

}
