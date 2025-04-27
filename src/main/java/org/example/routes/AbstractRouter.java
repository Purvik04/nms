package org.example.routes;

import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.example.service.DBService;
import org.example.utils.Constants;
import org.example.utils.Utils;

public abstract class AbstractRouter implements RouterHandler
{
    protected final Router router;
    protected final DBService dbService;

    protected AbstractRouter(Vertx vertx)
    {
        this.router = Router.router(vertx);

        this.dbService = new DBService(vertx);

        initRoutes();
    }

    protected void handleCreate(RoutingContext context)
    {
        context.request().bodyHandler(body ->
        {
            if (isInvalidBody(body.toString(), context)) return;

            dbService.create(body.toJsonObject(), context);
        });
    }

    protected void handleUpdate(RoutingContext context)
    {
        context.request().bodyHandler(body ->
        {
            var id = context.pathParam(Constants.ID);

            if (isInvalidId(id, context) || isInvalidBody(body.toString(), context)) return;

            dbService.update(id, body.toJsonObject(), context);
        });
    }

    protected void handleGetById(RoutingContext context)
    {
        var id = context.pathParam(Constants.ID);

        if (isInvalidId(id, context)) return;

        dbService.getById(id, context);
    }

    protected void handleDelete(RoutingContext context)
    {
        var id = context.pathParam(Constants.ID);

        if (isInvalidId(id, context)) return;

        dbService.delete(id, context);
    }

    private boolean isInvalidId(String id, RoutingContext context)
    {
        if (id == null || id.isEmpty())
        {
            context.response().setStatusCode(400).end(Constants.MESSAGE_ID_REQUIRED);

            return true;
        }

        return false;
    }

    private boolean isInvalidBody(String body, RoutingContext context)
    {
        if (body == null || body.isEmpty())
        {
            context.response().setStatusCode(400).end(Constants.MESSAGE_BODY_REQUIRED);

            return true;
        }
        else if (!Utils.validateRequest(new io.vertx.core.json.JsonObject(body), context))
        {
            context.response().setStatusCode(400).end(Constants.MESSAGE_INCORRECT_BODY);

            return true;
        }

        return false;
    }

    @Override
    public Router getRouter() {
        return router;
    }
}
