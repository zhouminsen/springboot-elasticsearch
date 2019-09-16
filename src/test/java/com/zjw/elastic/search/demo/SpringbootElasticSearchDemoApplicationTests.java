package com.zjw.elastic.search.demo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest
public class SpringbootElasticSearchDemoApplicationTests {

    @Autowired
    private RestHighLevelClient client;

    @Test
    public void contextLoads() throws IOException {
        String buyer = "lele";
        String date = "2019-04-07 11:14:00";
        Double total = 1888d;
        String products = "[" +
                "            {" +
                "                \"price\": \"1888\"," +
                "                \"count\": \"1\"," +
                "                \"name\": \"人头马\"," +
                "                \"category\": \"酒水类\"" +
                "            }" +
                "        ]";
        String id = "16";
        Map map = new HashMap();
        XContentBuilder builder = null;
        IndexRequest request = new IndexRequest("order");
        List<Product> productList = JSON.parseObject(products, new TypeReference<List<Product>>() {
        });
        List<Map<String, Object>> list = productList.stream().map(e -> {
            Map<String, Object> temp = new HashMap<>();
            Field[] fields = e.getClass().getDeclaredFields();
            for (Field f : fields) {
                f.setAccessible(true);
                try {
                    temp.put(f.getName(), f.get(e));
                } catch (IllegalAccessException e1) {
                    e1.printStackTrace();
                }
            }
            return temp;
        }).collect(Collectors.toList());
        try {
            builder = XContentFactory.jsonBuilder();
            builder.startObject()
                    .field("buyer", buyer).field("date", date)
                    .field("totalPrice", total)

                    .field("products", list)
                    .endObject()
            ;


            request.id(id).opType("create").source(builder);

            IndexResponse response = client.index(request, RequestOptions.DEFAULT);
            map.put("status", response.status());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
