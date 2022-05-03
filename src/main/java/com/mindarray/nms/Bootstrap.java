package com.mindarray.nms;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Bootstrap {
    public static final  Vertx vertx= Vertx.vertx();
    public static final Logger LOG= LoggerFactory.getLogger(Bootstrap.class);
    public static void main(String[] args) {
       start(APIServer.class.getName())
               .compose(future->start(DatabaseEngine.class.getName()))
               .compose(future->start(DiscoveryEngine.class.getName()))
               .compose(future->start(PollerEngine.class.getName()))
               .onComplete(handler ->{
                  if (handler.succeeded()){
                      LOG.debug("All Verticle deployed successfully");
                  }
                  else {LOG.error("ERROR "+handler.cause());
                  }
               });
    }
    public static Future<Void> start(String verticle){
        Promise promise= Promise.promise();
        vertx.deployVerticle(verticle,handler->{
            if (handler.succeeded()){
                promise.complete();
            }
            else{
                promise.fail(handler.cause());
            }
        });
        return promise.future();
    }
}
