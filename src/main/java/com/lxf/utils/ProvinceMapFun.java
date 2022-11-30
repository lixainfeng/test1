package com.lxf.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lxf.model.Access;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

/**
 * 按照省份维度对新老用户进行统计分析
 */
public class ProvinceMapFun extends RichMapFunction<Access,Access> {
    CloseableHttpClient httpClient;

    @Override
    public void open(Configuration parameters) throws Exception {
         httpClient = HttpClients.createDefault();
    }

    @Override
    public void close() throws Exception {
        if(httpClient != null){
            httpClient.close();
        }
    }



    @Override
    public Access map(Access access) throws Exception {
        String url = "https://restapi.amap.com/v3/ip?ip="+access.ip+"&output=json&key="+ StringUtils.code;
        String province = "";
        String city = "";
        CloseableHttpResponse response = null;
        try {
            HttpGet get = new HttpGet(url);
            response = httpClient.execute(get);
            int statusCode = response.getStatusLine().getStatusCode();
            if(statusCode == 200){
                HttpEntity entity = response.getEntity();
                String s = EntityUtils.toString(entity, "UTF-8");
                JSONObject jsonObject = JSON.parseObject(s);
                province = jsonObject.getString("province");
                city = jsonObject.getString("city");
                access.province = province;
                access.city = city;
            }
        }catch(Exception e){
            e.printStackTrace();
        }
        return access;
    }

}
