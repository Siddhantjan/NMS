package com.mindarray.nms;


import com.mindarray.Bootstrap;
import com.zaxxer.nuprocess.NuProcessBuilder;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class Utils {
    private Utils() {
    }
    private static final Logger LOG = LoggerFactory.getLogger(Utils.class.getName());
    public static Future<JsonObject> checkSystemStatus(JsonObject credential) {
        var errors = new ArrayList<String>();
        Promise<JsonObject> promise = Promise.promise();
        if ((!credential.containsKey("ip")) || credential.getString("ip") == null) {
            errors.add("IP address is null in check system status");
            LOG.error("IP address is null in check system status");
            promise.fail(errors.toString());
        } else {
            Bootstrap.vertx.executeBlocking(blockingHandler -> {
                var processBuilder = new NuProcessBuilder(Arrays.asList("fping", "-c", "3", "-t", "1000", "-q", credential.getString("ip")));
                var handler = new ProcessHandler();
                processBuilder.setProcessListener(handler);
                var process = processBuilder.start();
                handler.onStart(process);
                try {
                    process.waitFor(4000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException exception) {
                    errors.add(exception.getCause().getMessage());
                    Thread.currentThread().interrupt();
                }
                var result = handler.output();
                blockingHandler.complete(result);
            }).onComplete(completeHandler -> {
                var result = completeHandler.result().toString();
                if (result == null) {
                    errors.add("Request time out occurred");
                    LOG.error("Request time out occurred");
                } else {
                    var pattern = Pattern.compile("\\w+\\/\\w+\\/\\%\\w+ \\= \\d\\/\\d\\/(\\d)%");
                    var matcher = pattern.matcher(result);
                    if (matcher.find() && !matcher.group(1).equals("0")) {
                        errors.add("Loss percentage is :" + matcher.group(1));
                    }
                }
                if (errors.isEmpty()) {
                    promise.complete(credential);
                } else {
                    promise.fail(errors.toString());
                }
            });
        }
        return promise.future();
    }

    public static Future<JsonObject> checkPort(JsonObject credential){
        LOG.info("check port called...");
        var errors =new ArrayList<>();
        Promise<JsonObject> promise=Promise.promise();
        String ip = credential.getString("ip");
        int port=credential.getInteger("port");
        try {
            Socket s = new Socket(ip, port);


        }catch (Exception e){
            errors.add(e.getMessage());
        }
        if (errors.isEmpty()){
            promise.complete(new JsonObject().put(Constant.STATUS,Constant.SUCCESSFUL));
        }
        else {
            promise.fail(errors.toString());
        }
        return promise.future();
    }
    public static Future<JsonObject> spwanProcess(JsonObject credential) {
        var errors = new ArrayList<String>();
        Promise<JsonObject> promise = Promise.promise();
        if (credential == null) {
            errors.add("credential is null");
            LOG.error("credential is null");
            promise.fail(errors.toString());
        } else {
            Bootstrap.vertx.executeBlocking(blockingHandler->{
            String encoder = (Base64.getEncoder().encodeToString((credential).toString().getBytes(StandardCharsets.UTF_8)));
            var processBuilder = new NuProcessBuilder(Arrays.asList("./plugin.exe", encoder));
            var handler = new ProcessHandler();
            processBuilder.setProcessListener(handler);
            var process = processBuilder.start();

                handler.onStart(process);
                try {
                    process.waitFor(4, TimeUnit.SECONDS);
                } catch (Exception exception) {
                    errors.add(exception.getCause().getMessage());
                    Thread.currentThread().interrupt();
                }
                var handlerResult = handler.output();
                blockingHandler.complete(handlerResult);
            }).onComplete(completeHandler->{
                var result =completeHandler.result();
                if (result == null) {
                    errors.add("timeout occurred");
                }
                if (errors.isEmpty()) {
                    promise.complete(credential.put("result", result));
                } else {
                    promise.fail(errors.toString());
                }
            });
        }

        return promise.future();
    }
    public static JsonArray metric_values(String type){
        var metricData = new JsonArray();
        switch (type) {
            case "linux" -> {
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP,"ping").put("time",60000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "Cpu").put("time", 15000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "Disk").put("time", 55000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "Memory").put("time", 45000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "Process").put("time", 30000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "System").put("time", 60000));
            }
            case "windows" -> {
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP,"ping").put("time",60000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "Cpu").put("time", 17000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "Disk").put("time", 27000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "Memory").put("time", 22000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "Process").put("time", 12000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "System").put("time", 32000));
            }
            case "snmp" -> {
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP,"ping").put("time",60000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "System").put("time", 20000));
                metricData.add(new JsonObject().put(Constant.METRIC_GROUP, "Interface").put("time", 10000));
            }
        }
        return metricData;
    }
}
