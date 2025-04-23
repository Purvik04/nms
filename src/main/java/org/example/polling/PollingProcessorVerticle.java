package org.example.polling;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.example.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class PollingProcessorVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(PollingProcessorVerticle.class);

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.eventBus().consumer(Constants.EVENTBUS_POLLING_PROCESSOR_ADDRESS, this::handlePollingBatch);
        logger.info("⚙️ Polling Processor started and listening for batches");
        startPromise.complete();
    }

    private void handlePollingBatch(Message<JsonArray> message) {
        JsonArray deviceBatch = message.body();
        logger.info("📥 Received device batch of size: {}", deviceBatch.size());

        // Serialize the batch to string to pass to the Go plugin
        String inputJson = deviceBatch.encode();

        vertx.executeBlocking(promise -> {
            try {
                String pluginOutput = runGoPluginSync(inputJson);
                promise.complete(pluginOutput);
            } catch (Exception e) {
                promise.fail(e);
            }
        },false, res -> {
            if (res.succeeded()) {
                String pluginOutput = (String) res.result();

                JsonArray processedData;
                try {
                    processedData = new JsonArray(pluginOutput);
                } catch (Exception e) {
                    logger.error("❌ Failed to parse Go plugin output as JSON: {}", e.getMessage());
                    return;
                }

                logger.info("✅ Plugin processed batch, sending {} entries to DB", processedData.size());

                logger.info(processedData.toString());
                sendToDatabase(processedData);
            } else {
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

    private void sendToDatabase(JsonArray polledResults) {
        for (int i = 0; i < polledResults.size(); i++) {
            var deviceResult = polledResults.getJsonObject(i);

            var jobId = deviceResult.getInteger("id");
            var data = deviceResult.getJsonObject("data");
            var polledAt = deviceResult.getString("polled_at");

            var insertQuery = """
            INSERT INTO provisioned_data (job_id, data, polled_at)
            VALUES ($1, $2::jsonb, $3)
        """;

            var params = new JsonArray()
                    .add(jobId)
                    .add(data) // send JSONObject
                    .add(polledAt);

            var dbRequest = new JsonObject()
                    .put("query", insertQuery)
                    .put("params", params);

            vertx.eventBus().request(Constants.EVENTBUS_DATABASE_ADDRESS, dbRequest, reply -> {
                if (reply.succeeded()) {
                    var response = (JsonObject) reply.result().body();
                    if (response.getBoolean(Constants.SUCCESS)) {
                        logger.info("✅ Inserted provisioned data for job_id {}", jobId);
                    } else {
                        logger.error("❌ DB insert error for job_id {}: {}", jobId, response.getString(Constants.ERROR));
                    }
                } else {
                    logger.error("❌ Failed to send DB insert request for job_id {}: {}", jobId, reply.cause().getMessage());
                }
            });
        }
    }

}
