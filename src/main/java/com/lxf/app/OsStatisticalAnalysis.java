package com.lxf.app;

import com.alibaba.fastjson.JSON;
import com.lxf.model.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

/**
 * 按照操作系统维度进行统计分析
 */
public class OsStatisticalAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("42.192.196.73").build();
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
        }).filter(x -> x != null)
                .filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access access) throws Exception {
                        return "startup".equals(access.event);
                    }
                });
        // TODO...  新老用户  ==> wc
        filterSourrce.map(new MapFunction<Access, Tuple2<Integer,Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(Access access) throws Exception {
                return Tuple2.of(access.nu,1);
            }
        }).keyBy(x -> x.f0).sum(1).print().setParallelism(1);

        env.execute();

    }
}
