package com.mindarray.api;

import com.mindarray.Bootstrap;
import com.mindarray.nms.Constant;
import com.mindarray.nms.Utils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

public class Monitor {
    private static final Logger LOG = LoggerFactory.getLogger(Monitor.class.getName());

    public void init(Router router) {
        LOG.info("start -> {}", " Monitor Init Called .....");
        router.route().method(HttpMethod.POST).path(Constant.MONITOR_POINT + Constant.PROVISION_POINT).handler(this::validate).handler(this::create);
        router.route().method(HttpMethod.DELETE).path(Constant.MONITOR_POINT + "/:id/").handler(this::validate).handler(this::delete);
        router.route().method(HttpMethod.GET).path(Constant.MONITOR_POINT).handler(this::get);
        router.route().method(HttpMethod.GET).path(Constant.MONITOR_POINT + "/:id/").handler(this::validate).handler(this::getById);

    }

    private void getById(RoutingContext context) {
        LOG.info("monitor get by id called");
        var eventBus = Bootstrap.vertx.eventBus();
        var response = context.response();
        try {
            var query = "Select * from monitor where monitor_id =" + context.pathParam(Constant.ID) + ";";
            eventBus.<JsonObject>request(Constant.DATABASE_ADDRESS, new JsonObject().put("query", query).put(Constant.METHOD_TYPE, "get").put(Constant.REQUEST_CONTEXT, "monitor"), handler -> {
                if (handler.succeeded()) {
                    var result = handler.result().body().getValue("data");
                    response.setStatusCode(200).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.SUCCESSFUL).put("result", result).encodePrettily());
                } else {
                    response.setStatusCode(200).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, handler.cause().getMessage()).encodePrettily());
                }

            });


        } catch (Exception e) {
            response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.MESSAGE, "wrong json format").put(Constant.ERROR, e.getMessage()).encodePrettily());
        }
    }

    private void delete(RoutingContext context) {
        LOG.info("monitor delete called");
        var eventBus = Bootstrap.vertx.eventBus();
        var response = context.response();
        try {
            var query = "delete from monitor where monitor_id =" + context.pathParam(Constant.ID) + ";";
            eventBus.<JsonObject>request(Constant.DATABASE_ADDRESS, new JsonObject().put("query", query).put(Constant.METHOD_TYPE, "monitorDelete").put(Constant.REQUEST_CONTEXT, "monitor").put("monitor.id", context.pathParam(Constant.ID)), handler -> {
                var deleteResult = handler.result().body();
                if (deleteResult.getString(Constant.STATUS).equals(Constant.SUCCESSFUL)) {
                    response.setStatusCode(200).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE);
                    response.end(new JsonObject().put(Constant.MESSAGE, "id " + context.pathParam(Constant.ID) + " deleted").put(Constant.STATUS, Constant.SUCCESSFUL).encodePrettily());
                } else {
                    response.setStatusCode(200).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE);
                    response.end(new JsonObject().put(Constant.ERROR, handler.result().body()).put(Constant.STATUS, Constant.FAIL).encodePrettily());
                }

            });

        } catch (Exception e) {
            response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.MESSAGE, "wrong json format").put(Constant.ERROR, e.getMessage()).encodePrettily());
        }

    }

    private void get(RoutingContext context) {
        LOG.info("monitor get all called");
        var eventBus = Bootstrap.vertx.eventBus();
        var response = context.response();
        try {

            var query = "Select * from monitor;";
            eventBus.<JsonObject>request(Constant.DATABASE_ADDRESS, new JsonObject().put("query", query).put(Constant.METHOD_TYPE, "get").put(Constant.REQUEST_CONTEXT, "monitor"), handler -> {
                if (handler.succeeded()) {
                    var result = handler.result().body().getValue("data");
                    response.setStatusCode(200).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.SUCCESSFUL).put("result", result).encodePrettily());
                } else {
                    response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, handler.cause().getMessage()).encodePrettily());
                }

            });


        } catch (Exception e) {
            response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.MESSAGE, "wrong json format").put(Constant.ERROR, e.getMessage()).encodePrettily());
        }
    }

    private void create(RoutingContext context) {
        var eventBus = Bootstrap.vertx.eventBus();
        var response = context.response();

        LOG.info("monitor create called");
        var contextData = context.getBodyAsJson();
        String query = "insert into monitor (ip,host,type,port) values(" + "\"" + contextData.getString("ip") + "\",\"" + contextData.getString("host") + "\",\"" + contextData.getString("type") + "\"," + contextData.getInteger("port") + ");";
        eventBus.<JsonObject>request(Constant.DATABASE_ADDRESS, new JsonObject().put("query", query).put(Constant.METHOD_TYPE, Constant.CREATE).put(Constant.REQUEST_CONTEXT, "monitor"), handler -> {
            if (handler.succeeded()) {
                var result = handler.result().body().getJsonArray("data");
                var id = result.getJsonObject(0).getInteger(Constant.ID);
                response.setStatusCode(200).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.SUCCESSFUL).put(Constant.MESSAGE, "monitor created successfully").put("id", id).encodePrettily());

                metricGroupCreate(context.getBodyAsJson().put("monitor.id", id));
            } else {
                response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, handler.result().body()).encodePrettily());
            }
        });
    }

    private void metricGroupCreate(JsonObject monitorData) {
        var eventBus = Bootstrap.vertx.eventBus();

        Promise<JsonObject> promise = Promise.promise();
        Future<JsonObject> future = promise.future();
        String query = "";
        StringBuilder values = new StringBuilder();
        LOG.info("metric Group func called....");

        AtomicInteger i = new AtomicInteger();
        if (monitorData.getString("type").equals("snmp")) {
            var upObjects = new JsonArray();

            var getObjects = monitorData.getJsonObject("objects").getJsonArray("interfaces");
            getObjects.stream().map(JsonObject::mapFrom).forEach(value -> {
                if (!value.getString("interface.operational.status").equals("down")) {
                    upObjects.add(value);
                }
            });
            monitorData.getJsonObject("objects").put("interfaces", upObjects);
            var metricData = Utils.metric_values(monitorData.getString("type"));

            metricData.stream().map(JsonObject::mapFrom).forEach(value -> {
                metricData.getJsonObject(i.get()).mergeIn(monitorData);

                i.getAndIncrement();
            });
            metricData.stream().map(JsonObject::mapFrom).forEach(value -> values.append("(").append(value.getInteger("monitor.id")).append(",").append(value.getInteger("credential.id")).append(",").append("\"").append(value.getString("metric.group")).append("\",").append(value.getInteger("time")).append(",").append("\'").append(value.getJsonObject("objects")).append("\'").append("),"));
            query = "insert into metric(monitor_id,credential_profile,metric_group,time,objects)values" + values.substring(0, values.toString().length() - 1) + ";";
           eventBus.request(Constant.DATABASE_ADDRESS, new JsonObject().put("query", query).put(Constant.METHOD_TYPE, "metricGroupCreate"), handler -> {
                if (handler.succeeded()) {
                    LOG.info("metrics added");
                    promise.complete(monitorData);
                } else {
                    LOG.error(handler.cause().getMessage());
                    promise.fail("failed to add metrics");
                }
            });

        } else {
            var metricData = Utils.metric_values(monitorData.getString("type"));

            metricData.stream().map(JsonObject::mapFrom).forEach(value -> {
                metricData.getJsonObject(i.get()).mergeIn(monitorData);

                i.getAndIncrement();
            });
            metricData.stream().map(JsonObject::mapFrom).forEach(value ->
                    values.append("(").append(value.getInteger("monitor.id")).append(",").append(value.getInteger("credential.id"))
                            .append(",").append("\"")
                            .append(value.getString("metric.group"))
                            .append("\",").append(value.getInteger("time")).append("),"));
            query = "insert into metric(monitor_id,credential_profile,metric_group,time)values" + values.substring(0, values.toString().length() - 1) + ";";
           eventBus.request(Constant.DATABASE_ADDRESS, new JsonObject()
                    .put("query", query).put(Constant.METHOD_TYPE, "metricGroupCreate"), handler -> {
                if (handler.succeeded()) {
                    LOG.info("metrics added");
                    promise.complete(monitorData);
                } else {
                    LOG.error(handler.cause().getMessage());
                    promise.fail("failed to add metrics");
                }
            });


        }
        future.onComplete(completeHandler->{
            if(completeHandler.succeeded()){
                var getQuery = "select id,username,password,community,version,metric_group,time from credential,metric where metric.credential_profile= credential_id and credential_id="+ completeHandler.result().getInteger(Constant.CREDENTIAL_ID)+";";
              eventBus.<JsonObject>request(Constant.DATABASE_ADDRESS,completeHandler.result().put(Constant.METHOD_TYPE,"get").put("query",getQuery),resultHandler->{
                    if (resultHandler.succeeded()) {
                        var data = new JsonObject();
                        var pollingData = new JsonObject()
                                .put(Constant.IP, completeHandler.result().getString(Constant.IP))
                                .put(Constant.PORT, completeHandler.result().getInteger(Constant.PORT))
                                .put(Constant.TYPE, completeHandler.result().getString(Constant.TYPE))
                                .put("category", "polling");
                        resultHandler.result().body().getJsonArray("data").stream().map(JsonObject::mapFrom).forEach(value -> {
                            data.put(value.getString("id"), value.mergeIn(pollingData));
                            LOG.info("data -> {}",data);
                            eventBus.send(Constant.SCHEDULER_ENGINE, data);
                        });
                    }
                });
            }
            else{
                LOG.error("error:{}",completeHandler.cause().getMessage());

            }

        });

    }


    private void validate(RoutingContext context) {
        LOG.info("start-> {}", "Monitor Validate called...");

        var response = context.response();
        var eventBus = Bootstrap.vertx.eventBus();

        try {
            if ((context.request().method()!=HttpMethod.DELETE) && (context.request().method()!=HttpMethod.GET)) {
                if (context.getBodyAsJson() == null) {
                    response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.MESSAGE, "wrong json format").encodePrettily());
                }
                context.getBodyAsJson().stream().forEach(value -> {
                    if (context.getBodyAsJson().getValue(value.getKey()) instanceof String) {
                        context.getBodyAsJson().put(value.getKey(), context.getBodyAsJson().getString(value.getKey()).trim());
                    }
                });
                context.setBody(context.getBodyAsJson().toBuffer());
            }
            switch (context.request().method().toString()) {
                case "POST" -> validateCreate(context);
                case "DELETE", "GET" -> {
                    if (context.pathParam(Constant.ID) == null) {
                        response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.MESSAGE, "id is null").put(Constant.STATUS, Constant.FAIL).encodePrettily());
                    } else {
                        eventBus.<JsonObject>request(Constant.DATABASE_ADDRESS, new JsonObject().put(Constant.ID, Integer.parseInt(context.pathParam(Constant.ID))).put(Constant.REQUEST_CONTEXT, "monitor").put(Constant.METHOD_TYPE, Constant.CHECK_ID), validateHandler -> {
                            if (validateHandler.succeeded() || validateHandler.result().body() != null) {
                                if (validateHandler.result().body().getString(Constant.STATUS).equals(Constant.FAIL)) {
                                    response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.ERROR, validateHandler.result().body().getString(Constant.ERROR)).put(Constant.STATUS, Constant.FAIL).encodePrettily());
                                } else {
                                    context.next();
                                }
                            } else {
                                response.setStatusCode(500).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE);
                                response.end(new JsonObject().put(Constant.MESSAGE, "Internal Server Error Occurred").encodePrettily());

                            }
                        });
                    }
                }
                default ->
                        response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.MESSAGE, "wrong method").encodePrettily());
            }

        } catch (Exception e) {
            response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE).end(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.MESSAGE, "wrong json format").put(Constant.ERROR, e.getMessage()).encodePrettily());
        }

    }

    private void validateCreate(RoutingContext context) {
        LOG.info("monitor validate create called");

        var errors = new ArrayList<String>();
        var eventBus = Bootstrap.vertx.eventBus();
        var response = context.response();

        if (!context.getBodyAsJson().containsKey(Constant.IP) || context.getBodyAsJson().getString(Constant.IP).isEmpty()) {
            errors.add("ip is required");
            LOG.error("ip is required");
        }
        if (!context.getBodyAsJson().containsKey(Constant.TYPE) || context.getBodyAsJson().getString(Constant.TYPE).isEmpty()) {
            errors.add("type is required");
            LOG.error("type is required");
        }
        if (!context.getBodyAsJson().containsKey(Constant.CREDENTIAL_ID) || context.getBodyAsJson().getInteger(Constant.CREDENTIAL_ID) == null) {
            errors.add("credential.id is required");
            LOG.error("credential.id is required");
        }
        if (!context.getBodyAsJson().containsKey(Constant.PORT) || context.getBodyAsJson().getInteger(Constant.PORT) == null) {
            errors.add("port is required");
            LOG.error("port is required");
        }
        if (!context.getBodyAsJson().containsKey("host") || context.getBodyAsJson().getString("host").isEmpty()) {
            errors.add("hostname required");
            LOG.error("hostname required");
        }
        if (context.getBodyAsJson().getString(Constant.TYPE).equals("snmp") && (!context.getBodyAsJson().containsKey("objects") || context.getBodyAsJson().isEmpty())) {
            errors.add("objects are required for snmp devices");
            LOG.error("objects are required for snmp devices");
        }
        if (errors.isEmpty()) {

            eventBus.<JsonObject>request(Constant.DATABASE_ADDRESS, context.getBodyAsJson().put(Constant.REQUEST_CONTEXT, "monitor").put(Constant.METHOD_TYPE, Constant.MONITOR_CHECK), validateHandler -> {
                if (validateHandler.succeeded()) {

                    var responseData = validateHandler.result().body();

                    if (responseData.getString(Constant.STATUS).equals(Constant.SUCCESSFUL)) {
                        context.next();
                    } else {
                        response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE);
                        response.end(new JsonObject().put(Constant.ERROR, responseData.getString(Constant.ERROR)).put(Constant.STATUS, Constant.FAIL).encodePrettily());
                    }

                } else {
                    response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE);
                    response.end(new JsonObject().put(Constant.ERROR, validateHandler.cause().getMessage()).put(Constant.STATUS, Constant.FAIL).encodePrettily());

                }
            });

        } else {
            response.setStatusCode(400).putHeader(Constant.CONTENT_NAME, Constant.CONTENT_TYPE);
            response.end(new JsonObject().put(Constant.MESSAGE, errors).put(Constant.STATUS, Constant.FAIL).encodePrettily());
        }
    }
}
