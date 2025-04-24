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

import java.io.*;

public class PollingProcessorVerticle extends AbstractVerticle
{
    private static final Logger logger = LoggerFactory.getLogger(PollingProcessorVerticle.class);

    @Override
    public void start(Promise<Void> startPromise)
    {
        vertx.eventBus().consumer(Constants.EVENTBUS_POLLING_PROCESSOR_ADDRESS, this::handlePollingBatch);

        startPromise.complete();
    }

    private void handlePollingBatch(Message<JsonArray> message)
    {
        var deviceBatch = message.body();

        logger.info("📥 Received device batch of size: {}", deviceBatch.size());

        vertx.executeBlocking(promise ->
        {
            var pluginOutput = Utils.runGoPlugin(deviceBatch, "metrics");

            promise.complete(pluginOutput);
        },false, res ->
        {
            if (res.succeeded())
            {
                var pluginOutput = (String) res.result();

                JsonArray processedData;
                try
                {
                    processedData = new JsonArray(pluginOutput);
                }
                catch (Exception e)
                {
                    logger.error("❌ Failed to parse Go plugin output as JSON: {}", e.getMessage());

                    return;
                }

                logger.info("✅ Plugin processed batch, sending {} entries to DB", processedData.size());

                sendToDatabase(processedData);
            }
            else
            {
                logger.error("❌ Go plugin execution failed: {}", res.cause().getMessage());
            }
        });
    }

    private String runGoPluginSync(String inputJson) throws IOException, InterruptedException {
        ProcessBuilder builder = new ProcessBuilder("go/ssh-plugin" , "metrics"); // Make sure binary is in working directory
        Process process = builder.start();

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()))) {
            writer.write(inputJson);
            writer.flush();
        }

        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(line);
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("Plugin exited with non-zero code: " + exitCode);
        }

        return output.toString();
    }

    private void sendToDatabase(JsonArray polledResults)
    {
        var batchParams = new JsonArray();

        for (int i = 0; i < polledResults.size(); i++){

            var deviceResult = polledResults.getJsonObject(i);

            var jobId = deviceResult.getInteger("id");

            var data = deviceResult.getJsonObject("data");

            var polledAt = deviceResult.getString("polled_at");

            batchParams.add(new JsonArray()
                    .add(jobId)
                    .add(data) // send JSONObject
                    .add(polledAt));
        }

        var insertQuery = """
                INSERT INTO provisioned_data (job_id, data, polled_at)
                VALUES ($1, $2::jsonb, $3)
            """;

        var request = new JsonObject()
                .put("query", insertQuery)
                .put("params", batchParams);

        vertx.eventBus().request(Constants.EVENTBUS_BATCH_UPDATE_ADDRESS, request, reply -> {
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
