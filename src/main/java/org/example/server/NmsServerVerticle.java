package org.example.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.example.routes.CredentialRouter;
import org.example.routes.DiscoveryRouter;
import org.example.routes.ProvisioningRouter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NmsServerVerticle extends AbstractVerticle {

    private static final Logger logger = LoggerFactory.getLogger(NmsServerVerticle.class);

    @Override
    public void start(Promise<Void> startPromise)
    {
        Router mainRouter = Router.router(vertx);

        mainRouter.route("/").handler(BodyHandler.create());

        var credentialRouter = new CredentialRouter(vertx);

        mainRouter.route("/credentials/*").subRouter(credentialRouter.getCredentialRouter());

        var discoveryRouter = new DiscoveryRouter(vertx);

        mainRouter.route("/discovery/*").subRouter(discoveryRouter.getDiscoveryRouter());

        var provisioningRouter = new ProvisioningRouter(vertx);

        mainRouter.route("/provision/*").subRouter(provisioningRouter.getProvisioningRouter());

        vertx.createHttpServer()
                .requestHandler(mainRouter)
                .listen(8080, result ->
                {
                    if (result.succeeded())
                    {
                        logger.info("NMS Server is running on port 8080");
                    }
                    else
                    {
                        logger.error("Failed to start NMS Server: {}",result.cause().getMessage());
                    }
                });

        startPromise.complete();
    }
}
