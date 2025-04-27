package org.example.routes;

import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import org.example.utils.Constants;

public class DiscoveryRouter extends AbstractRouter
{
    public DiscoveryRouter(Vertx vertx)
    {
        super(vertx);
    }

    @Override
    public void initRoutes()
    {
        router.post("/createDiscoveryProfile").handler(this::handleCreate);

        router.post("/run").handler(this::handleRunDiscovery);

        router.get("/getAll").handler(dbService::getAll);

        router.get("/:id").handler(this::handleGetById);

        router.put("/:id").handler(this::handleUpdate);

        router.delete("/:id").handler(this::handleDelete);
    }

    private void handleRunDiscovery(RoutingContext context)
    {
        context.request().bodyHandler(body ->
        {
            if (body == null || body.length() == 0)
            {
                context.response().setStatusCode(400).end(Constants.MESSAGE_BODY_REQUIRED);
            }
            else
            {
                dbService.runDiscovery(body.toJsonObject().getJsonArray("ids"), context);
            }
        });
    }
}


