package com.yjp.stage.origin.sample;

import org.apache.kudu.Type;
import org.apache.kudu.client.RowResult;

/**
 * Created by lichanghong on 2019/11/28.
 * Description:
 */
public class KuduUtil {

    public static Object getObject(RowResult result, Type type, String name,String destination) {
        Object obj = null;
        switch (type) {
            case STRING:
                if(result.isNull(name)){
                    break;
                }
                obj = result.getString(name);
                break;
            case FLOAT:
                if(result.isNull(name)){
                    break;
                }
                obj = result.getFloat(name);
                break;
            case INT8:
                if(result.isNull(name)){
                    break;
                }
                obj = result.getByte(name);
                break;
            case INT16:
                if(result.isNull(name)){
                    break;
                }
                obj = result.getShort(name);
                break;
            case INT32:
                if(result.isNull(name)){
                    break;
                }
                obj = result.getInt(name);
                break;
            case INT64:
                if(result.isNull(name)){
                    break;
                }
                obj = result.getLong(name);
                break;
            case DOUBLE:
                if(result.isNull(name)){
                    break;
                }
                obj = result.getDouble(name);
                break;
            case BOOL:
                if(result.isNull(name)){
                    break;
                }
                obj = result.getBoolean(name);
                break;
            case UNIXTIME_MICROS:
                if(result.isNull(name)){
                    break;
                }
                if("mysql".equals(destination)){
                    obj = TimeUtil.getStringFromTimestamp(result.getTimestamp(name));
                }else{
                    obj = result.getTimestamp(name);
                }
                break;
            case BINARY:
                if(result.isNull(name)){
                    break;
                }
                obj = result.getBinary(name);
                break;
            default:
                throw new IllegalArgumentException("Illegal var type: " + type);
        }

        return obj;
    }

}
