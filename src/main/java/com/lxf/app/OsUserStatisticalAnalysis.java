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
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 按照操作系统维度进行新老用户的统计分析
 */
public class OsUserStatisticalAnalysis {
    private static final Logger LOG = LoggerFactory.getLogger(OsUserStatisticalAnalysis.class);
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
        /**
         * 针对两个维度：操作系统+新老用户进行wc操作
         */
        // TODO... 操作系统维度  新老用户  ==> wc
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> result = filterSourrce.map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(Access value) throws Exception {
                return Tuple3.of(value.os, value.nu, 1);
            }
        }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        }).sum(2);


        result.addSink(new RedisSink<Tuple3<String, Integer, Integer>>(conf, new RedisExampleMapper()));

        env.execute();
    }
    public static class RedisExampleMapper implements RedisMapper<Tuple3<String, Integer, Integer>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
        }

        @Override
        public String getKeyFromData(Tuple3<String, Integer, Integer> data) {
            return data.f0 + "_" + data.f1;
        }

        @Override
        public String getValueFromData(Tuple3<String, Integer, Integer> data) {
            return data.f2+"";
        }


    }
}
