package com.mindarray.nms;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

public class DiscoveryEngine extends AbstractVerticle {
    @Override
    public void start(Promise<Void> startPromise) {
        startPromise.complete();
    }
}
