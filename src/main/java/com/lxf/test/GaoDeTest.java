package com.lxf.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxf.model.Access;
import com.lxf.utils.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;


public class GaoDeTest {
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
        filterSourrce.map(new MapFunction<Access, Access>() {
            @Override
            public Access map(Access access) throws Exception {
                String url = "https://restapi.amap.com/v3/ip?ip="+access.ip+"&output=json&key="+ StringUtils.code;

                CloseableHttpClient httpClient = HttpClients.createDefault();
                CloseableHttpResponse response = null;

                try {
                    HttpGet get = new HttpGet(url);
                    response = httpClient.execute(get);
                    int statusCode = response.getStatusLine().getStatusCode();
                    if(statusCode == 200){
                        HttpEntity entity = response.getEntity();
                        String s = EntityUtils.toString(entity, "UTF-8");
                        JSONObject jsonObject = JSON.parseObject(s);
                        String province = jsonObject.getString("province");
                        String city = jsonObject.getString("city");
                        //System.out.println(province + '\t' + city);
                        access.province = province;
                        access.city = city;
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }finally {
                    if(null != response) try {
                        response.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
                return access;
            }
        }).print();

        env.execute();
    }
}
