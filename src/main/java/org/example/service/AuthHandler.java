package org.example.service;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;

public class AuthHandler {

    private final JWTAuth jwtAuth;

    public AuthHandler(Vertx vertx)
    {
        this.jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions()
                        .addPubSecKey(new PubSecKeyOptions()
                        .setAlgorithm("HS256")
                        .setBuffer("secret-key-which-should-be-very-strong")));  // keep this secret safe
    }

    public String generateToken(JsonObject claims)
    {
        return jwtAuth.generateToken(claims, new JWTOptions().setExpiresInMinutes(60)); // 1-hour expiry
    }

    public JWTAuth getProvider()
    {
        return jwtAuth;
    }
}

