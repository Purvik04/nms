package org.example.routes;

import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import org.example.server.DBService;
import org.example.utils.Constants;

public class ProvisioningRouter {

    private final Router router;

    private final DBService dbService;

    public ProvisioningRouter(Vertx vertx)
    {
        this.router = Router.router(vertx);

        this.dbService = new DBService(vertx);

        router.post("/startProvision/:id").handler(context ->
        {
            var id = context.pathParam("id");

            if (id == null || id.isEmpty())
            {
                context.response().setStatusCode(400).end(Constants.MESSAGE_ID_REQUIRED);
            }
            else
            {
                dbService.addForProvision(id, context);
            }
        });

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

    public Router getProvisioningRouter()
    {
        return router;
    }
}
