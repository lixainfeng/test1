package com.lxf.utils;


import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class ProvinceSinkFun extends RichSinkFunction<Tuple3<String,Integer,Integer>> {
    Connection conn;
    PreparedStatement updatepsmt;
    PreparedStatement insertpsmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        conn = MysqlUtil.getConnection();
        insertpsmt = conn.prepareStatement("insert into test3(province,nu,counts) values(?,?,?)");
        updatepsmt = conn.prepareStatement("update test3 set counts = ? where province = ? and nu = ?");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(insertpsmt != null) insertpsmt.close();
        if(updatepsmt != null) updatepsmt.close();
        if(conn != null) conn.close();

    }

    @Override
    public void invoke(Tuple3<String, Integer, Integer> value, Context context) throws Exception {
        System.out.println(value.f0+"---"+value.f1+"====>"+value.f1);
        updatepsmt.setInt(1, value.f2);
        updatepsmt.setString(2, value.f0);
        updatepsmt.setInt(3, value.f1);
        updatepsmt.execute();

        if(updatepsmt.getUpdateCount() == 0){
            insertpsmt.setString(1, value.f0);
            insertpsmt.setInt(2, value.f1);
            insertpsmt.setInt(3, value.f2);
            insertpsmt.execute();
        }
    }

}

