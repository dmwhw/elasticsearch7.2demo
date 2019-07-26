package com.haowen;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.get.GetResult;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class RestClientHelper {
    private static RestHighLevelClient client;
    private static final RequestOptions COMMON_OPTIONS;
    static {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        // builder.addHeader("Authorization", "Bearer " + "");
        builder.setHttpAsyncResponseConsumerFactory(
            new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(30 * 1024 * 1024));
        COMMON_OPTIONS = builder.build();
    }

    RestClientHelper() {
        client = new RestHighLevelClient(RestClient.builder(new HttpHost(EsConsts.HOST_NAME, EsConsts.PORT, "http")));
    }

    public static RestHighLevelClient getClient() {
        if (client == null) {
            synchronized (RestClientHelper.class) {
                if (client == null) {
                    RestClientHelper restClientHelper = new RestClientHelper();
                }
            }
        }
        return client;
    }

    // --------------创建索引start--------------------------------------------------------------------------
    public CreateIndexResponse createIndex() throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(EsConsts.INDEX_NAME);
        buildSetting(request);
        buildIndexMapping(request);
        CreateIndexResponse createIndexResponse = client.indices().create(request, COMMON_OPTIONS);
        return createIndexResponse;
    }

    // 设置分片
    public void buildSetting(CreateIndexRequest request) {
        request.settings(Settings.builder().put("index.number_of_shards", 3).put("index.number_of_replicas", 2));
    }

    // 设置index的mapping
    public void buildIndexMapping(CreateIndexRequest request) {
        // Map<String, Object> jsonMap = new HashMap<>();

        Map<String, Object> number = new HashMap<>();
        number.put("type", "text");
        Map<String, Object> price = new HashMap<>();
        price.put("type", "float");
        Map<String, Object> title = new HashMap<>();
        title.put("type", "text");
        Map<String, Object> province = new HashMap<>();
        province.put("type", "text");
        Map<String, Object> publishTime = new HashMap<>();
        publishTime.put("type", "date");
        Map<String, Object> properties = new HashMap<>();

        properties.put("number", number);
        properties.put("price", price);
        properties.put("title", title);
        properties.put("province", province);
        properties.put("publishTime", publishTime);

        Map<String, Object> book = new HashMap<>();
        book.put("properties", properties);
        // jsonMap.put("books", book);

        request.mapping(EsConsts.TYPE, book);
    }

    public void buildIndexMapping2(CreateIndexRequest request) {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.startObject();
            {

                builder.startObject("properties");

                {
                    builder.startObject("message");
                    {
                        builder.field("type", "text");
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
            builder.endObject();
            request.mapping(EsConsts.TYPE, builder);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // --------------创建索引end--------------------------------------------------------------------------

    // --------------删除索引start--------------------------------------------------------------------------

    /**
     * 官方7.0废弃了type
     * 
     * @author haowen
     * @time 2019年7月26日上午11:35:51
     * @Description
     */

    // 判断存在并删除
    public void deleteIndex() throws IOException {
        GetIndexRequest getIndexRequest = new GetIndexRequest();
        getIndexRequest.indices(EsConsts.INDEX_NAME);
        if (client.indices().exists(getIndexRequest, COMMON_OPTIONS)) {
            DeleteIndexRequest deleteIndexRequest = new DeleteIndexRequest(EsConsts.INDEX_NAME);
            client.indices().delete(deleteIndexRequest, COMMON_OPTIONS);
        }
    }
    // --------------删除索引end--------------------------------------------------------------------------

    // --------------添加数据start--------------------------------------------------------------------------

    public void add(Book book) {
        try {
            IndexRequest insertDataRequest = getInsertDataRequest(book);
            client.index(insertDataRequest, COMMON_OPTIONS);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    // --------------添加数据end--------------------------------------------------------------------------

    // --------------获取数据start--------------------------------------------------------------------------

    public Book getData(String id) throws IOException {
        GetRequest getRequest = new GetRequest(EsConsts.INDEX_NAME, id);
        GetResponse getResponse = client.get(getRequest, COMMON_OPTIONS);
        byte[] sourceAsBytes = getResponse.getSourceAsBytes();
        ObjectMapper mapper = new ObjectMapper();
        Book book = mapper.readValue(sourceAsBytes, Book.class);
        return book;
    }
    // --------------获取数据end--------------------------------------------------------------------------

    // --------------更新数据start--------------------------------------------------------------------------

    public GetResult updateData(Book book) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(EsConsts.INDEX_NAME, book.getNumber());
        updateRequest.doc(getInsertDataRequest(book));
        GetResult getResult = client.update(updateRequest, COMMON_OPTIONS).getGetResult();
        return getResult;
    }

    private IndexRequest getInsertDataRequest(Book book) throws JsonProcessingException {
        IndexRequest indexRequest = new IndexRequest(EsConsts.INDEX_NAME, EsConsts.TYPE, book.getNumber());
        ObjectMapper mapper = new ObjectMapper();
        byte[] json = mapper.writeValueAsBytes(book);
        indexRequest.source(json, XContentType.JSON);
        return indexRequest;
    }
    // --------------更新数据end--------------------------------------------------------------------------

    // 注：es的更新数据，不论是直接用script方式，还是updaterequest.doc方式，貌似都是在原来已有的数据上合并（涉及到的字段更新，update中未涉及到的字段保持不变）；如果需要全量覆盖，直接用添加数据请求。

    // --------------删除数据start--------------------------------------------------------------------------

    public String deleteData(String id) throws IOException {
        DeleteRequest deleteRequest = new DeleteRequest(EsConsts.INDEX_NAME, id);
        DeleteResponse deleteResponse = client.delete(deleteRequest, COMMON_OPTIONS);
        return deleteResponse.getResult().toString();
    }
    // --------------删除数据end--------------------------------------------------------------------------

}
