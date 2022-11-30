package com.lxf.app;

import com.alibaba.fastjson.JSON;
import com.lxf.model.Access;

import com.lxf.utils.ProvinceMapFun;
import com.lxf.utils.ProvinceSinkFun;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 按照省份维度对新老用户进行统计分析,并将结果写入mysql中
 */
public class ProvinceUserStatisticalAnalysis {
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
        }).filter(x -> x != null)
                .filter(new FilterFunction<Access>() {
                    @Override
                    public boolean filter(Access access) throws Exception {
                        return "startup".equals(access.event);
                    }
                });
        KeyedStream<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>> keyBySource = filterSourrce.map(new ProvinceMapFun())
                .map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(Access access) throws Exception {
                        return Tuple3.of(access.province, access.nu, 1);
                    }
                })
                .keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                });
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> resoult = keyBySource.sum(2).setParallelism(1);
        resoult.addSink(new ProvinceSinkFun());

        env.execute();
    }
}
