package com.mindarray.nms;

import com.mysql.cj.jdbc.result.ResultSetMetaData;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

public class DatabaseEngine extends AbstractVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(DatabaseEngine.class.getName());

    @Override
    public void start(Promise<Void> startPromise) {
        init();
        var eventBus = vertx.eventBus();
        eventBus.<JsonObject>localConsumer(Constant.DATABASE_ADDRESS, databaseHandler -> {
            var databaseData = databaseHandler.body();
            var futures = new ArrayList<Future>();

            switch (databaseData.getString(Constant.METHOD_TYPE)) {

                case Constant.ALL_CHECK -> {
                    if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("discovery")) {
                        if (databaseData.containsKey(Constant.DISCOVERY_NAME)) {
                            var nameCheck = check("discovery", "discovery_name", databaseData.getString(Constant.DISCOVERY_NAME).trim());
                            futures.add(nameCheck);
                        }
                        if (databaseData.containsKey(Constant.CREDENTIAL_PROFILE)) {
                            var credentialID = check("credential", "credential_id", databaseData.getValue(Constant.CREDENTIAL_PROFILE));
                            futures.add(credentialID);
                        }
                    } else if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("credential")) {
                        var nameCheck = check("credential", "credential_name", databaseData.getString(Constant.CREDENTIAL_NAME).trim());
                        futures.add(nameCheck);
                    }
                    CompositeFuture.join(futures).onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, completeHandler.cause().getMessage()));

                        } else {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.SUCCESSFUL));
                        }
                    });
                }
                case Constant.DISCOVERY_UPDATE -> {
                    if (databaseData.containsKey(Constant.DISCOVERY_NAME)) {
                        var nameCheck = check("discovery", "discovery_name", databaseData.getString(Constant.DISCOVERY_NAME).trim());
                        futures.add(nameCheck);
                    }
                    if (databaseData.containsKey(Constant.DISCOVERY_ID)) {
                        var idCheck = check("discovery", "discovery_id", databaseData.getString(Constant.DISCOVERY_ID));
                        futures.add(idCheck);
                    }
                    if (databaseData.containsKey(Constant.CREDENTIAL_PROFILE)) {
                        var credentialCheck = check("credential", "credential_id", databaseData.getString(Constant.CREDENTIAL_PROFILE));
                        futures.add(credentialCheck);
                    }
                    CompositeFuture.join(futures).onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, completeHandler.cause().getMessage()));

                        } else {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.SUCCESSFUL));
                        }
                    });
                }
                case Constant.CREDENTIAL_UPDATE -> {
                    if (databaseData.containsKey(Constant.CREDENTIAL_ID)) {
                        if (databaseData.containsKey(Constant.CREDENTIAL_ID)) {
                            var idChecker = check("credential", "credential_id", databaseData.getInteger(Constant.CREDENTIAL_ID));
                            futures.add(idChecker);
                        }
                        if (databaseData.containsKey(Constant.CREDENTIAL_NAME)) {
                            var nameChecker = check("credential", "credential_name", databaseData.getString(Constant.CREDENTIAL_NAME).trim());
                            futures.add(nameChecker);
                        }
                    }
                    CompositeFuture.join(futures).onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, completeHandler.cause().getMessage()));
                        } else {
                            String query = "select * from credential where credential_id=" + databaseData.getInteger("credential.id") + ";";
                            var data = getQuery(query);
                            data.onComplete(queryCompleteHandler -> {
                                if (queryCompleteHandler.failed()) {
                                    databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, queryCompleteHandler.cause().getMessage()));

                                } else {
                                    databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.SUCCESSFUL));
                                }
                            });
                        }
                    });

                }
                case Constant.CHECK_ID -> {

                    if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("discovery")) {
                        var idCheck = check("discovery", "discovery_id", databaseData.getString(Constant.ID));
                        futures.add(idCheck);
                    }
                    if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("monitor")) {
                        var idCheck = check("monitor", "monitor_id", databaseData.getString(Constant.ID));
                        futures.add(idCheck);
                    }
                    if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("credential")) {
                        var checkInCredential = check("credential", "credential_id", databaseData.getInteger(Constant.ID));
                        futures.add(checkInCredential);
                    }
                    CompositeFuture.join(futures).onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, completeHandler.cause().getMessage()));

                        } else {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.SUCCESSFUL));
                        }
                    });
                }
                case Constant.CREDENTIAL_DELETE -> {
                    if (databaseData.containsKey(Constant.ID)) {
                        var checkInCredential = check("credential", "credential_id", databaseData.getInteger(Constant.ID));
                        var checkInDiscovery = check("discovery", "credential_profile", databaseData.getInteger(Constant.ID));

                        futures.add(checkInCredential);
                        futures.add(checkInDiscovery);
                        CompositeFuture.join(futures).onComplete(completeHandler -> {
                            if (completeHandler.failed()) {
                                if (futures.get(0).failed()) {
                                    databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, completeHandler.cause().getMessage()));
                                } else {
                                    databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.SUCCESSFUL));
                                }
                            } else {
                                databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, "credential.id exists in discovery"));
                            }
                        });
                    }
                }
                case Constant.MONITOR_CHECK -> {
                    String ipQuery = "select exists(select * from monitor where ip=" + "\"" + databaseData.getString("ip") + "\"" + ") as existsValue;";
                    var checkIp = getQuery(ipQuery);
                    checkIp.onComplete(ipCompleteHandler -> {
                        if (ipCompleteHandler.failed()) {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, ipCompleteHandler.cause().getMessage()));
                        } else {
                            var checkIPData = ipCompleteHandler.result().getJsonArray("data");
                            if (checkIPData.getJsonObject(0).containsKey("existsValue") && checkIPData.getJsonObject(0).getInteger("existsValue").equals(0)) {
                                String query = "select exists(select discovery_id from discovery where ip=" + "\"" + databaseData.getString("ip") + "\"" + " and Json_search(result,\"one\",\"success\") and credential_profile=" + databaseData.getInteger("credential.id") + " and port=" + databaseData.getInteger("port") + " and type=" + "\"" + databaseData.getString("type") + "\"" + ") as existsValue;";
                                var executeQuery = getQuery(query);
                                executeQuery.onComplete(queryCompleteHandler -> {
                                    if (queryCompleteHandler.failed()) {
                                        databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, queryCompleteHandler.cause().getMessage()));
                                    } else {
                                        var checkData = queryCompleteHandler.result().getJsonArray("data");
                                        if (checkData.getJsonObject(0).containsKey("existsValue") && !checkData.getJsonObject(0).getInteger("existsValue").equals(0)) {
                                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.SUCCESSFUL).put(Constant.MESSAGE, queryCompleteHandler.result()));
                                        } else {
                                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, "data mismatched"));
                                        }
                                    }
                                });
                            } else {
                                databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, "monitor already exists"));
                            }

                        }
                    });
                }
                case Constant.CREATE -> {
                    var execute = executeQuery(databaseData.getString("query"));
                    execute.onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, completeHandler.cause().getMessage()));
                        } else {
                            if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("discovery")) {
                                String query = "select max(discovery_id) as id  from discovery;";
                                var getId = getQuery(query);
                                getId.onComplete(queryCompleteHandler -> {
                                    if (queryCompleteHandler.failed()) {
                                        databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, queryCompleteHandler.cause().getMessage()));

                                    } else {
                                        databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.SUCCESSFUL));
                                    }
                                });

                            } else if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("credential")) {
                                String query = "select max(credential_id) as id  from credential;";
                                var getId = getQuery(query);
                                getId.onComplete(queryCompleteHandler -> {
                                    if (queryCompleteHandler.failed()) {
                                        databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, queryCompleteHandler.cause().getMessage()));

                                    } else {
                                        databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.SUCCESSFUL));
                                    }
                                });
                            } else if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("monitor")) {
                                String query = "select max(monitor_id) as id  from monitor;";
                                var getId = getQuery(query);
                                getId.onComplete(queryCompleteHandler -> {
                                    if (queryCompleteHandler.failed()) {
                                        databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, queryCompleteHandler.cause().getMessage()));

                                    } else {
                                        databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.SUCCESSFUL));
                                    }
                                });
                            }
                        }
                    });
                }
                case "delete", "update", "metricGroupCreate" -> {
                    var execute = executeQuery(databaseData.getString("query"));
                    execute.onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            databaseHandler.reply(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, completeHandler.cause().getMessage()));
                        } else {
                            databaseHandler.reply(new JsonObject().put(Constant.STATUS, Constant.SUCCESSFUL));
                        }
                    });
                }
                case "get" -> {
                    var execute = getQuery(databaseData.getString("query"));
                    execute.onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            databaseHandler.reply(completeHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, completeHandler.cause().getMessage()));
                        } else {
                            databaseHandler.reply(completeHandler.result());
                        }
                    });

                }
                case "run" -> {
                    String query = "select c.username,c.password,c.community,c.version,d.ip,d.type,d.port from discovery as d JOIN credential as c on d.credential_profile=c.credential_id where d.discovery_id=" + databaseData.getValue(Constant.ID) + ";";
                    var getData = getQuery(query);
                    getData.onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            databaseHandler.reply(completeHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, completeHandler.cause().getMessage()));

                        } else {
                            var result = completeHandler.result();
                            eventBus.<JsonObject>request(Constant.DISCOVERY_ADDRESS, result, discoveryHandler -> {
                                if (discoveryHandler.failed()) {
                                    databaseHandler.reply(completeHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, discoveryHandler.cause().getMessage()));
                                } else {
                                    var discoveryData = discoveryHandler.result().body();

                                    String insertDiscoveryData = "update discovery set result =" + "\'" + discoveryData.getValue("result") + "\'" + " where discovery_id =" + databaseData.getValue(Constant.ID) + ";";
                                    var execute = executeQuery(insertDiscoveryData);
                                    execute.onComplete(queryCompleteHandler -> {
                                        if (queryCompleteHandler.failed()) {
                                            databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, queryCompleteHandler.cause().getMessage()));

                                        } else {
                                            databaseHandler.reply(new JsonObject().put(Constant.MESSAGE, "id " + databaseHandler.body().getValue("id") + " Discovery Successful"));


                                        }
                                    });
                                }
                            });
                        }

                    });
                }
                case "monitorDelete" -> {
                    var execute = executeQuery(databaseData.getString("query"));
                    execute.onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            databaseHandler.reply(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, completeHandler.cause().getMessage()));
                        } else {
                            var id = databaseData.getValue("monitor.id");
                            String deleteMetricData = "delete from metric where monitor_id =" + id + ";";
                            var deleteQuery = executeQuery(deleteMetricData);
                            deleteQuery.onComplete(queryCompletedHandler -> {
                                if (queryCompletedHandler.failed()) {
                                    databaseHandler.reply(new JsonObject().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, queryCompletedHandler.cause().getMessage()));
                                } else {
                                    databaseHandler.reply(new JsonObject().put(Constant.STATUS, Constant.SUCCESSFUL));
                                    LOG.info("metric data also deleted");
                                }
                            });
                        }
                    });
                }
                default ->
                        databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, "wrong method"));

            }
        });
        startPromise.complete();

    }

    private Future<JsonObject> executeQuery(String query) {
        var errors = new ArrayList<>();
        Promise<JsonObject> promise = Promise.promise();
        vertx.<JsonObject>executeBlocking(queryHandler -> {
            var result = new JsonObject();
            try (var conn = connection(); var smt = conn.createStatement()) {
                smt.execute("use nms");

                var res = smt.execute(query);
                result.put("result", res);
            } catch (SQLException sqlException) {
                errors.add(sqlException);
            }
            queryHandler.complete(result);
        }).onComplete(completeHandler -> {
            if (errors.isEmpty()) {
                promise.complete(completeHandler.result());

            } else {
                promise.fail(String.valueOf(errors));
            }
        });
        return promise.future();
    }

    private Future<JsonObject> getQuery(String query) {
        var errors = new ArrayList<>();
        Promise<JsonObject> promise = Promise.promise();
        vertx.<JsonObject>executeBlocking(queryHandler -> {
            var resultData = new JsonObject();
            var data = new JsonArray();
            try (var conn = connection(); var smt = conn.createStatement()) {
                smt.execute("use nms");
                ResultSet res = smt.executeQuery(query);
                ResultSetMetaData rsmd = (ResultSetMetaData) res.getMetaData();
                while (res.next()) {
                    var result = new JsonObject();
                    int columns = rsmd.getColumnCount();
                    int i = 1;
                    while (i <= columns) {
                        if (res.getObject(i) != null) {
                            String key = rsmd.getColumnName(i).replace("_", ".");
                            result.put(key, res.getObject(i));
                        }
                        i++;
                    }

                    data.add(result);

                }
            } catch (SQLException sqlException) {
                errors.add(sqlException);
            }
            resultData.put("data", data);
            queryHandler.complete(resultData);

        }).onComplete(completeHandler -> {
            if (errors.isEmpty()) {
                promise.complete(completeHandler.result());

            } else {
                promise.fail(String.valueOf(errors));
            }
        });
        return promise.future();
    }

    private Future<JsonObject> check(String table, String column, Object value) {
        var errors = new ArrayList<>();
        Promise<JsonObject> promise = Promise.promise();
        if (table == null || column == null || value == null) {
            errors.add("data is null");
        } else {
            vertx.<JsonObject>executeBlocking(queryHandler -> {
                try (var conn = connection(); var smt = conn.createStatement()) {

                    smt.execute("use nms");
                    String query = "select exists(select * from " + table + " where " + column + "=" + "\"" + value + "\"" + ");";
                    ResultSet result = smt.executeQuery(query);
                    while (result.next()) {
                        if (column.equals("discovery_name") || column.equals("credential_name")) {
                            if (result.getInt(1) == 1) {
                                errors.add(table + "." + column + " is not unique");
                            }
                        } else {
                            if (result.getInt(1) == 0) {
                                errors.add(table + "." + column + " does not exists in table ");
                            }
                        }
                    }
                } catch (SQLException sqlException) {
                    errors.add(sqlException.getCause().getMessage());
                }
                queryHandler.complete();
            }).onComplete(completeHandler -> {
                if (errors.isEmpty()) {
                    promise.complete(new JsonObject().put(Constant.STATUS, Constant.SUCCESSFUL));
                } else {
                    promise.fail(String.valueOf(errors));
                }
                LOG.info("check Function Complete");

            });
        }
        return promise.future();
    }

    private void init() {
        LOG.info("DatabaseEngine init called...");
        try (var conn = connection(); var smt = conn.createStatement()) {
            smt.execute("CREATE DATABASE IF NOT EXISTS nms;");
            smt.execute("use nms");
            smt.execute("create table if not exists discovery(discovery_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, discovery_name varchar(255) NOT NULL UNIQUE, ip varchar(90) NOT NULL, type varchar(90) NOT NULL, credential_profile int NOT NULL, port int NOT NULL, result json);");
            smt.execute("create table if not exists monitor(monitor_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, ip varchar(90) NOT NULL, host varchar(255) NOT NULL ,type varchar(90) NOT NULL,  port int NOT NULL);");
            smt.execute("CREATE TABLE IF NOT EXISTS credential (credential_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, credential_name varchar(255) NOT NULL UNIQUE,protocol varchar(90) NOT NULL, username varchar(255), password varchar(255), community varchar(90), version varchar(50));");
            smt.execute("create table if not exists metric(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,monitor_id int,credential_profile int,metric_group varchar(90),time int,objects json);");

        } catch (SQLException sqlException) {
            LOG.error(sqlException.getCause().getMessage());
        }

    }

    private Connection connection() throws SQLException {
        LOG.info("connection called...");
        var connection = DriverManager.getConnection("jdbc:mysql://localhost:3306", "siddhant", "Sid@mtdt#25");
        LOG.info("Database Connection Successful");
        return connection;
    }
}
