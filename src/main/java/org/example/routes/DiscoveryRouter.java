package org.example.routes;

import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import org.example.service.DBService;
import org.example.utils.Constants;
import org.example.utils.Utils;

public class DiscoveryRouter {

    private final Router router;

    private final DBService dbService;

    public DiscoveryRouter(Vertx vertx)
    {
        this.router = Router.router(vertx);

        this.dbService = new DBService(vertx);

        router.post("/createDiscoveryProfile").handler(context ->

                context.request().bodyHandler(body ->
                {
                    if(body == null || body.length() == 0)
                    {
                        context.response().setStatusCode(400).end(Constants.MESSAGE_BODY_REQUIRED);
                    }
                    else if (!Utils.validateRequest(body.toJsonObject(), context))
                    {
                        context.response().setStatusCode(400).end(Constants.MESSAGE_INCORRECT_BODY);
                    }
                    else
                    {
                        dbService.create(body.toJsonObject(), context);
                    }
                }));

        router.post("/run/").handler(context ->

            context.request().bodyHandler(body ->
            {
                if(body == null || body.length() == 0)
                {
                    context.response().setStatusCode(400).end(Constants.MESSAGE_BODY_REQUIRED);
                }
                else
                {
                    dbService.runDiscovery(body.toJsonObject().getJsonArray("ids") , context);
                }
            }));

        router.get("/getAll").handler(dbService::getAll);

        router.get("/:id").handler(context ->
        {
            var id = context.pathParam("id");

            if (id == null || id.isEmpty())
            {
                context.response().setStatusCode(400).end(Constants.MESSAGE_ID_REQUIRED);
            }
            else
            {
                dbService.getById(id, context);
            }
        });

        router.put("/:id").handler(context ->

                context.request().bodyHandler(body ->
                {
                    if(body == null || body.length() == 0)
                    {
                        context.response().setStatusCode(400).end(Constants.MESSAGE_BODY_REQUIRED);
                    }
                    else if (context.pathParam("id") == null || context.pathParam("id").isEmpty())
                    {
                        context.response().setStatusCode(400).end(Constants.MESSAGE_ID_REQUIRED);
                    }
                    else if (!Utils.validateRequest(body.toJsonObject(), context))
                    {
                        context.response().setStatusCode(400).end(Constants.MESSAGE_INCORRECT_BODY);
                    }
                    else
                    {
                        dbService.update(context.pathParam("id"), body.toJsonObject(), context);
                    }
                }));

        router.delete("/:id").handler(context ->
        {
            var id = context.pathParam("id");

            if (id == null || id.isEmpty())
            {
                context.response().setStatusCode(400).end(Constants.MESSAGE_ID_REQUIRED);
            }
            else
            {
                dbService.delete(id, context);
            }
        });
    }

    public Router getDiscoveryRouter()
    {
        return router;
    }
}


