package org.example.routes;

import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import org.example.server.DBService;
import org.example.utils.Constants;

public class CredentialRouter {

    private final Router router;

    private final DBService dbService;

    public CredentialRouter(Vertx vertx)
    {
        this.router = Router.router(vertx);

        this.dbService = new DBService(vertx);

        router.post("/createCredential").handler(context ->

                context.request().bodyHandler(body ->
                {
                    if(body == null || body.length() == 0)
                    {
                        context.response().setStatusCode(400).end(Constants.MESSAGE_BODY_REQUIRED);
                    }
                    else
                    {
                       dbService.create(body.toJsonObject(), context);
                    }

                }));

        router.get("/getCredentials").handler(dbService::getAll);

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
                    if (context.pathParam("id") == null || context.pathParam("id").isEmpty())
                    {
                        context.response().setStatusCode(400).end(Constants.MESSAGE_ID_REQUIRED);
                    }
                    else if(body == null || body.length() == 0)
                    {
                        context.response().setStatusCode(400).end(Constants.MESSAGE_BODY_REQUIRED);
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

    public Router getCredentialRouter()
    {
        return router;
    }
}


