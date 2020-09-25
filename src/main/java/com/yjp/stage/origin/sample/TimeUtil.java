package com.yjp.stage.origin.sample;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by lichanghong on 2019/11/26.
 * Description:
 */
public class TimeUtil {
    public static Timestamp getFiveMinuteBefore(String time,String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = null;

        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        Calendar instance = Calendar.getInstance();
        instance.setTime(date);
        //将时间提前了5分钟，是为冗余数据，防止数据丢失
        instance.add(Calendar.MINUTE, -5);
        //将时间增加8个小时，是因为kudu中保存的时间戳是东八区的时间戳
        instance.add(Calendar.HOUR,8);

        date = instance.getTime();
        return Timestamp.valueOf(sdf.format(date));
    }

    public static Timestamp getKuduTimestampFromLong(long time){
        Date date = new Date(time);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        Calendar instance = Calendar.getInstance();
        instance.setTime(date);
        instance.add(Calendar.HOUR,8);

        date = instance.getTime();
        return Timestamp.valueOf(sdf.format(date));
    }


    public static String getTimeFormatFromLong(long time){
        Date date = new Date(time);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String format = sdf.format(date);

        return format;
    }


    public static Timestamp getTimestampFromLong(long time){
        Date date = new Date(time);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return Timestamp.valueOf(sdf.format(date));
    }

    public static String getStringFromTimestamp(Timestamp timestamp){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Calendar instance = Calendar.getInstance();
        instance.setTime(timestamp);
        instance.add(Calendar.HOUR,-8);
        return sdf.format(instance.getTime());
    }


}
