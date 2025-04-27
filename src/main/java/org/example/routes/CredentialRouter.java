package org.example.routes;

import io.vertx.core.Vertx;

public class CredentialRouter extends AbstractRouter
{
    public CredentialRouter(Vertx vertx)
    {
        super(vertx);
    }

    @Override
    public void initRoutes()
    {
        router.post("/createCredential").handler(this::handleCreate);

        router.get("/getCredentials").handler(dbService::getAll);

        router.get("/:id").handler(this::handleGetById);

        router.put("/:id").handler(this::handleUpdate);

        router.delete("/:id").handler(this::handleDelete);
    }
}
