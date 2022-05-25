package com.mindarray.nms;

import com.mindarray.api.Monitor;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class PollerEngine extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(PollerEngine.class.getName());
    @Override
    public void start(Promise<Void> startPromise) {
        HashMap<String,String> pingData = new HashMap<>();
        var eventBus = vertx.eventBus();
        eventBus.<JsonObject>localConsumer(Constant.POLLING_ADDRESS, handler->{
           var poll=handler.body();
           LOG.info(poll.encodePrettily());
        });

        startPromise.complete();
    }
}
