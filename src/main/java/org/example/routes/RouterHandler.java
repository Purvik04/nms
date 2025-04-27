package org.example.routes;

import io.vertx.ext.web.Router;

public interface RouterHandler
{
    Router getRouter();

    void initRoutes();
}

