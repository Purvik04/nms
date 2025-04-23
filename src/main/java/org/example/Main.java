package org.example;

import io.vertx.core.Vertx;
import org.example.polling.PollingProcessorVerticle;
import org.example.polling.PollingSchedulerVerticle;
import org.example.server.DBVerticle;
import org.example.server.NmsServerVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static final Vertx vertx = Vertx.vertx();

    public static void main(String[] args)
    {
        vertx.deployVerticle(new NmsServerVerticle())
                .compose(res -> {

                    logger.info("NMS Server Verticle started successfully");

                    return vertx.deployVerticle(new DBVerticle());
                })
//                .compose(res-> {
//
//                    logger.info("DB Verticle started successfully");
//
//                    return vertx.deployVerticle(new PollingProcessorVerticle());
//                })
//                .compose(res-> {
//
//                    logger.info("processor Verticle started successfully");
//
//                    return vertx.deployVerticle(new PollingSchedulerVerticle());
//                })
                .onSuccess(res-> {

                    logger.info("scheduler Verticle started successfully");

                    logger.info("All Verticles started successfully");
                })
                .onFailure(err -> {

                    logger.error(err.getMessage());

                    vertx.close();
                });
    }

}