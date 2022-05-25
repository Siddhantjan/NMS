package com.mindarray.nms;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;

public class DiscoveryEngine extends AbstractVerticle {
    @Override
    public void start(Promise<Void> startPromise) {
        var eventBus = vertx.eventBus();
        eventBus.<JsonObject>localConsumer(Constant.DISCOVERY_ADDRESS, discoveryHandler -> {
            var discoveryData = discoveryHandler.body().getJsonArray("data");
            var jsonData = discoveryData.getJsonObject(0).put("category", "discovery");
            var pingCheck = Utils.checkSystemStatus(jsonData);
            pingCheck.onComplete(pingHandler -> {
                if (pingHandler.failed()) {
                    discoveryHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, pingHandler.cause().getMessage());

                } else {
                    var connectionCheck = Utils.spwanProcess(jsonData);
                    connectionCheck.onComplete(connectionHandler -> {
                        if (connectionHandler.failed()) {
                            discoveryHandler.reply(discoveryHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, connectionHandler.cause().getMessage()));

                        } else {
                            discoveryHandler.reply(connectionHandler.result());
                        }

                    });
                }

            });

        });
        startPromise.complete();
    }
}
