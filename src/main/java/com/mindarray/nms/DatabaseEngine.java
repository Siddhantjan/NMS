package com.mindarray.nms;

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
      var eventBus=vertx.eventBus();
      eventBus.<JsonObject>localConsumer("discovery_database",databaseHandler->{
          var databaseData=databaseHandler.body();
          switch (databaseData.getString("method")){
              case "checkUniqueName":{
                  var futures = new ArrayList<Future>();
                  var errors = new ArrayList<String>();
                  if (databaseData.containsKey("discovery.name")){
                      var nameCheck=check("discovery","name",databaseData.getString("discovery.name"));
                      futures.add(nameCheck);
                  }
                  if (databaseData.containsKey("discovery.ip")){
                      var ipCheck=check("discovery","ip",databaseData.getString("ip"));
                      futures.add(ipCheck);
                  }
                  if (databaseData.containsKey("credential.profile")){
                      var credentialID=check("credential","id",databaseData.getValue("credential.profile"));
                      futures.add(credentialID);
                  }

                  CompositeFuture.join(futures).onComplete(completeHandler->{
                      if(completeHandler.failed()) {
                          errors.add(completeHandler.cause().getMessage());
                      }
                      if(errors.isEmpty()){
                          databaseHandler.reply(databaseHandler.body().put(Constant.STATUS,Constant.SUCCESSFUL));
                      }else{
                          databaseHandler.reply(databaseHandler.body().put(Constant.STATUS,Constant.UNSUCCESSFUL).put(Constant.ERROR,errors.toString()));
                      }
                  });
                  break;
              }
          }
      });
        startPromise.complete();

    }

private Future<JsonObject> check(String table, String column, Object value){
        var errors= new ArrayList<>();
        Promise<JsonObject> promise = Promise.promise();
    if( table ==null || column == null || value ==null){
        errors.add("data is null");
    }else {
        vertx.<JsonObject>executeBlocking(queryHandler -> {
            try (var conn = connection()) {
                var smt = conn.createStatement();
                smt.execute("use nms");
                String query = "select exists(select * from " + table + " where " + column + "=" + "\"" + value + "\"" + ");";
                ResultSet result = smt.executeQuery(query);
                while (result.next()) {
                    if (column.equals("name")) {
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
        try (var conn=connection()){
            var smt=conn.createStatement();
            smt.execute("CREATE DATABASE IF NOT EXISTS nms;");
            smt.execute("use nms");
            smt.execute("create table if not exists discovery(id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name varchar(255) NOT NULL UNIQUE, ip varchar(90) NOT NULL, type varchar(90) NOT NULL, credential_profile int NOT NULL, port int NOT NULL);");
            smt.execute("CREATE TABLE IF NOT EXISTS credential (id INT NOT NULL AUTO_INCREMENT PRIMARY KEY, name varchar(255) NOT NULL UNIQUE,protocol varchar(90) NOT NULL, username varchar(255), password varchar(255), community varchar(90), version varchar(50));"
            );
        }
        catch (SQLException sqlException){
            LOG.error(sqlException.getCause().getMessage());
        }
    }

    private Connection connection() throws SQLException {
        LOG.info("connection called...");
        var connection= DriverManager.getConnection("jdbc:mysql://localhost:3306", "siddhant", "Sid@mtdt#25");
        LOG.info("Database Connection Successful");
        return connection;
    }
}
