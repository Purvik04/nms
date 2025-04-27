package org.example.server;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import org.example.routes.CredentialRouter;
import org.example.routes.DiscoveryRouter;
import org.example.routes.ProvisioningRouter;
import org.example.service.AuthHandler;
import org.example.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NmsServerVerticle extends AbstractVerticle
{
    private static final Logger logger = LoggerFactory.getLogger(NmsServerVerticle.class);

    @Override
    public void start(Promise<Void> startPromise)
    {
        var mainRouter = Router.router(vertx);

        var authHandler = new AuthHandler(vertx);

        mainRouter.route("/").handler(BodyHandler.create());

        // Public login route
        mainRouter.post("/login").handler(context ->

            context.request().bodyHandler(buffer -> {

                var body = buffer.toJsonObject();

                var username = body.getString("username");

                var password = body.getString("password");

                //Fake authentication for demo purposes
                if ("admin".equals(username) && "password".equals(password))
                {
                    var token = authHandler.generateToken(new JsonObject().put("username", username));

                    context.response().setStatusCode(401).end(new JsonObject().put(Constants.SUCCESS, true).put("token", token).encode());
                }
                else
                {
                    context.response().setStatusCode(401).end(new JsonObject().put(Constants.SUCCESS, false).put("message", "Invalid credentials").encode());
                }
            })
        );

        mainRouter.route("/api/*")
                .handler(JWTAuthHandler.create(authHandler.getProvider()))
                .failureHandler(context ->
                {
                    logger.error("Authentication failed: {}", context.failure().getMessage());

                    context.response().setStatusCode(401).end(new JsonObject().put("success", false).put("message", "Unauthorized").encode());
                });


        var credentialRouter = new CredentialRouter(vertx);

        mainRouter.route("/api/credentials/*").subRouter(credentialRouter.getRouter());

        var discoveryRouter = new DiscoveryRouter(vertx);

        mainRouter.route("/api/discovery/*").subRouter(discoveryRouter.getRouter());

        var provisioningRouter = new ProvisioningRouter(vertx);

        mainRouter.route("/api/provision/*").subRouter(provisioningRouter.getRouter());

        vertx.createHttpServer()
                .requestHandler(mainRouter)
                .listen(8080, result ->
                {
                    if (result.succeeded())
                    {
                        startPromise.complete();
                    }
                    else
                    {
                        startPromise.fail(result.cause().getMessage());
                    }
                });
    }
}
