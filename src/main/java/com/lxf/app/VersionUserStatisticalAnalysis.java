package com.lxf.app;

import com.alibaba.fastjson.JSON;
import com.lxf.model.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 按照version维度统计新老用户
 */
public class VersionUserStatisticalAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.readTextFile("/Users/shuofeng/IdeaProjects/flink-datastream-project/src/main/java/com/lxf/data/access.log");
        SingleOutputStreamOperator<Access> filterSourrce = source.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String s) throws Exception {
                try {
                    //将json数据转换成Access对象
                    Access access = JSON.parseObject(s, Access.class);
                    return access;
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }
            }
        }).filter(x -> x != null);

        filterSourrce.map(new MapFunction<Access, Tuple3<String,Integer,Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Access access) throws Exception {
                return Tuple3.of(access.version,access.nu,1);
            }
        }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f0,value.f1);
            }
        }).sum(2).print();

        env.execute();
    }
}
