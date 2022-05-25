package com.mindarray.nms;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class MetricScheduler extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(MetricScheduler.class.getName());

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        LOG.info("monitor scheduler vertical called");
        ConcurrentHashMap<String, JsonObject> context = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, JsonObject> originalPoller = new ConcurrentHashMap<>();
        var eventBus = vertx.eventBus();
        eventBus.<JsonObject>localConsumer(Constant.SCHEDULER_ENGINE, schedulerHandler -> {
            schedulerHandler.body().forEach(value ->{
                originalPoller.put(value.getKey(), schedulerHandler.body().getJsonObject(value.getKey()));
                var map = new ConcurrentHashMap<>(schedulerHandler.body().getJsonObject(value.getKey()).getMap());
                context.put(value.getKey(), JsonObject.mapFrom(map));
            });
        });
        vertx.setPeriodic(10000,handler -> {
            if(originalPoller.size()>0){
                originalPoller.forEach( (key,value)->{
                    var remainingTime = value.getInteger("time")-10000;
                    if(remainingTime <=0){
                        var originalTime =  context.get(key).getInteger("time");
                        value.put("time",originalTime);
                        eventBus.send(Constant.POLLING_ADDRESS,value);
                    }else{
                        value.put("time",remainingTime);
                    }
                });
            }
                });
        startPromise.complete();
    }
}
