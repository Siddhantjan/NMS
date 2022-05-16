package com.mindarray.nms;

import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Objects;

public class Utlis {

    public static JsonObject validate_credential(JsonObject credentials) {
            var errors=new ArrayList<String>();
            if(!credentials.containsKey(Constant.IP_ADDRESS)){
                errors.add("ip.address not found");
            }
            if(!credentials.containsKey(Constant.METRIC_TYPE)){
                errors.add("metric.type not found");
            }
            else{
                if(Objects.equals(credentials.getString(Constant.METRIC_TYPE).trim(),"linux")){
                    if(!credentials.containsKey(Constant.USERNAME))
                    {
                        errors.add("username not found");
                    }else if(!credentials.containsKey(Constant.PASSWORD)){
                        errors.add("password not found");
                    }
                }
                else if(Objects.equals(credentials.getString(Constant.METRIC_TYPE).trim(), "windows")){
                    if(!credentials.containsKey(Constant.USERNAME))
                    {
                        errors.add("username not found");
                    }
                    if(!credentials.containsKey(Constant.PASSWORD)){
                        errors.add("password not found");
                    }
                }
                else if(Objects.equals(credentials.getString(Constant.METRIC_TYPE).trim(), "network")){
                    if(!credentials.containsKey(Constant.VERSION))
                    {
                        errors.add("version not found");

                    }
                    if(!credentials.containsKey(Constant.COMMUNITY)){
                        errors.add("community not found");
                    }
                }
                else{
                    errors.add("metric.type not matched");
                }
            }
            if(errors.isEmpty()){
                credentials.put(Constant.STATUS, Constant.SUCCESSFUL);
            }else{
                credentials.put(Constant.STATUS, Constant.UNSUCCESSFUL);
                credentials.put(Constant.ERROR,errors);
            }

            return credentials;
        }

}
