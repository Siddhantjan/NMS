package com.mindarray.nms;

import com.mysql.cj.jdbc.result.ResultSetMetaData;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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
            var errors = new ArrayList<>();
            var futures = new ArrayList<Future>();
            switch (databaseData.getString(Constant.METHOD_TYPE)) {

                case Constant.ALL_CHECK -> {
                    if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("discovery")) {
                        if (databaseData.containsKey(Constant.DISCOVERY_NAME)) {
                            var nameCheck = check("discovery", "discovery_name", databaseData.getString(Constant.DISCOVERY_NAME));
                            futures.add(nameCheck);
                        }
                        if (databaseData.containsKey(Constant.CREDENTIAL_PROFILE)) {
                            var credentialID = check("credential", "credential_id", databaseData.getValue(Constant.CREDENTIAL_PROFILE));
                            futures.add(credentialID);
                        }
                    } else if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("credential")) {
                        var nameCheck = check("credential", "credential_name", databaseData.getString(Constant.CREDENTIAL_NAME));
                        futures.add(nameCheck);
                    }
                    CompositeFuture.join(futures).onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            errors.add(completeHandler.cause().getMessage());
                        }
                        if (errors.isEmpty()) {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.SUCCESSFUL));
                        } else {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, errors.toString()));
                        }
                    });
                }
                case Constant.DISCOVERY_UPDATE -> {
                    if (databaseData.containsKey(Constant.DISCOVERY_NAME)) {
                        var nameCheck = check("discovery", "discovery_name", databaseData.getString(Constant.DISCOVERY_NAME));
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
                            errors.add(completeHandler.cause().getMessage());
                        }
                        if (errors.isEmpty()) {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.SUCCESSFUL));
                        } else {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, errors.toString()));
                        }
                    });
                }
                case Constant.CREDENTIAL_UPDATE -> {
                    if (databaseData.containsKey(Constant.CREDENTIAL_ID)) {
                        var idChecker = check("credential", "credential_id",
                                databaseData.getInteger(Constant.CREDENTIAL_ID));

                        var nameChecker = check("credential", "credential_name",
                                databaseData.getString(Constant.CREDENTIAL_NAME));
                        futures.add(idChecker);
                        futures.add(nameChecker);
                    }
                    CompositeFuture.join(futures).onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            errors.add(completeHandler.cause().getMessage());
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, errors.toString()));
                        } else {
                            String query = "select * from credential where credential_id=" + databaseData.getInteger("credential.id") + ";";
                            var data = getQuery(query);
                            data.onComplete(queryCompleteHandler -> {
                                if (queryCompleteHandler.failed()) {
                                    errors.add(queryCompleteHandler.cause().getMessage());
                                }
                                if (errors.isEmpty()) {
                                    databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.SUCCESSFUL));
                                } else {
                                    databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, errors.toString()));
                                }
                            });
                        }
                    });

                }
                case Constant.CHECK_ID -> {

                    if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("discovery")) {
                        var idCheck = check("discovery", "discovery_id", databaseData.getString(Constant.ID));
                        futures.add(idCheck);
                    } else if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("credential")) {
                        var checkInCredential = check("credential", "credential_id", databaseData.getInteger(Constant.ID));
                        futures.add(checkInCredential);
                    }
                    CompositeFuture.join(futures).onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            errors.add(completeHandler.cause().getMessage());
                        }
                        if (errors.isEmpty()) {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.SUCCESSFUL));
                        } else {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, errors.toString()));
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
                                    errors.add(completeHandler.cause().getMessage());
                                    databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, errors.toString()));
                                } else {
                                    databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.SUCCESSFUL));
                                }
                            } else {

                                errors.add("credential.id exists in discovery");
                                databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, errors.toString()));

                            }
                        });
                    }
                }
                case "create" -> {
                    var execute = executeQuery(databaseData.getString("query"));
                    execute.onComplete(completeHandler -> {
                        if (completeHandler.failed()) {
                            databaseHandler.reply(databaseHandler.body().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, errors.toString()));
                        } else {
                            if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("discovery")) {
                                String query = "select max(discovery_id) as id  from discovery;";
                                var getId = getQuery(query);
                                getId.onComplete(queryCompleteHandler -> {
                                    if (queryCompleteHandler.failed()) {
                                        errors.add(queryCompleteHandler.cause().getMessage());
                                    }
                                    if (errors.isEmpty()) {
                                        databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.SUCCESSFUL));
                                    } else {
                                        databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, errors.toString()));
                                    }
                                });

                            }
                            else if (databaseData.getString(Constant.REQUEST_CONTEXT).equals("credential")) {
                                String query = "select max(credential_id) as id  from credential;";
                                var getId = getQuery(query);
                                getId.onComplete(queryCompleteHandler -> {
                                    if (queryCompleteHandler.failed()) {
                                        errors.add(queryCompleteHandler.cause().getMessage());
                                    }
                                    if (errors.isEmpty()) {
                                        databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.SUCCESSFUL));
                                    } else {
                                        databaseHandler.reply(queryCompleteHandler.result().put(Constant.STATUS, Constant.FAIL).put(Constant.ERROR, errors.toString()));
                                    }
                                });
                            }

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
                promise.complete(completeHandler.result().put(Constant.STATUS, Constant.SUCCESSFUL));

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
            var result = new JsonObject();
            try (var conn = connection(); var smt = conn.createStatement()) {
                smt.execute("use nms");
                ResultSet res = smt.executeQuery(query);
                ResultSetMetaData rsmd = (ResultSetMetaData) res.getMetaData();
                while (res.next()) {
                    int columns = rsmd.getColumnCount();
                    int i = 1;
                    while (i <= columns) {
                        String key = rsmd.getColumnName(i).replace("_", ".");
                        result.put(key, res.getObject(i));
                        i++;
                    }
                }
            } catch (SQLException sqlException) {
                errors.add(sqlException);
            }
            queryHandler.complete(result);
        }).onComplete(completeHandler -> {
            if (errors.isEmpty()) {
                promise.complete(completeHandler.result().put(Constant.STATUS, Constant.SUCCESSFUL));

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
            smt.execute("create table if not exists discovery(discovery_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, discovery_name varchar(255) NOT NULL UNIQUE, ip varchar(90) NOT NULL, type varchar(90) NOT NULL, credential_profile int NOT NULL, port int NOT NULL);");
            smt.execute("CREATE TABLE IF NOT EXISTS credential (credential_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, credential_name varchar(255) NOT NULL UNIQUE,protocol varchar(90) NOT NULL, username varchar(255), password varchar(255), community varchar(90), version varchar(50));"
            );
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
