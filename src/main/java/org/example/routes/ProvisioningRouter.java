package org.example.routes;

import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import org.example.utils.Constants;

public class ProvisioningRouter extends AbstractRouter
{
    public ProvisioningRouter(Vertx vertx)
    {
        super(vertx);
    }

    @Override
    public void initRoutes()
    {
        router.post("/startProvision/:id").handler(this::handleStartProvision);

        router.get("/getAll").handler(dbService::getAll);

        router.delete("/:id").handler(this::handleDelete);
    }

    private void handleStartProvision(RoutingContext context)
    {
        var id = context.pathParam(Constants.ID);

        if (id == null || id.isEmpty())
        {
            context.response().setStatusCode(400).end(Constants.MESSAGE_ID_REQUIRED);
        } else {
            dbService.addForProvision(id, context);
        }
    }
}