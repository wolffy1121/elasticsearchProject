package com.wolffy.es;

import com.alibaba.fastjson.JSON;
import com.apple.laf.AquaTreeUI;
import com.wolffy.es.bean.Movie;
import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class EsTest01 {

    private static RestHighLevelClient esClient = createClient();

    public static RestHighLevelClient createClient() {

        HttpHost[] httpHosts = {
                new HttpHost("hadoop102", 9200),
                new HttpHost("hadoop103", 9200),
                new HttpHost("hadoop104", 9200)
        };

        RestClientBuilder restClientBuilder = RestClient
                .builder(httpHosts);
        RestHighLevelClient restHighLevelClient = new RestHighLevelClient(restClientBuilder);


        return restHighLevelClient;
    }

    public void close( ) throws IOException {
        if (esClient != null) {
            esClient.close();
        }
    }

    public static void main(String[] args) throws IOException {
        System.out.println(esClient);
//        putTest();
//        postTest();
        bulkTest();
        esClient.close();
    }


    /**
     * 幂等写
     * @throws IOException
     */
    public static void putTest() throws IOException {

        Movie movie = new Movie("1001", "red sea action");
        String indexName = "movie_2022";

        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.id("1001");
        indexRequest.source(JSON.toJSON(movie), XContentType.JSON);

        esClient.index(indexRequest, RequestOptions.DEFAULT);


    }

    /**
     * 非幂等写
     * @throws IOException
     */
    public static void postTest() throws IOException {

        Movie movie = new Movie("1002", "red sea action");
        String indexName = "movie_2022";

        IndexRequest indexRequest = new IndexRequest(indexName);
        indexRequest.source(JSON.toJSON(movie), XContentType.JSON);

        esClient.index(indexRequest, RequestOptions.DEFAULT);

    }

    /**
     * 批量写入(幂等 非幂等)
     * @throws IOException
     */
    public static void bulkTest() throws IOException {

        Movie movie1 = new Movie("1003", "水门桥");
        Movie movie2 = new Movie("1004", "长金湖");

        ArrayList<Movie> movies = new ArrayList<>();
        movies.add(movie1);
        movies.add(movie2);

        String globalIndex = "movie_2022";
        BulkRequest bulkRequest = new BulkRequest(globalIndex);
        for (Movie movie : movies) {
            IndexRequest indexRequest = new IndexRequest();
            indexRequest.id(movie.getId());
            indexRequest.source(JSON.toJSONString(movie), XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        esClient.bulk(bulkRequest, RequestOptions.DEFAULT);

    }


    /**
     * updateById
     * @throws IOException
     */
    public static void updateById() throws IOException {

        UpdateRequest updateRequest = new UpdateRequest("movie_2022","1004");
        updateRequest.doc("movieName", "babai");

        esClient.update(updateRequest, RequestOptions.DEFAULT);

    }

    /**
     * updateByQuery
     * @throws IOException
     */
    public static void updateByQuery() throws IOException {

        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest("movie_2022");

        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("name.keyword", "red");
        updateByQueryRequest.setQuery(termQueryBuilder);
        HashMap<String, Object> map = new HashMap<>();
        map.put("newName","red");
        Script script = new Script(ScriptType.INLINE,
                Script.DEFAULT_SCRIPT_LANG,
                "ctx._source['movie_name']=map.newName",
                map);
        updateByQueryRequest.setScript(script);
        esClient.updateByQuery(updateByQueryRequest, RequestOptions.DEFAULT);

    }


    /**
     * 删除数据
     * @throws IOException
     */
    public static void deleteById() throws IOException {

        DeleteRequest deleteRequest = new DeleteRequest("movie_2022","1002");
        esClient.delete(deleteRequest, RequestOptions.DEFAULT);
    }

    public static void deleteByQuery() throws IOException {

        DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest("movie_2022");


        TermQueryBuilder termQueryBuilder = QueryBuilders.termQuery("moveName.keywords", "red");
        deleteByQueryRequest.setQuery(termQueryBuilder);
        esClient.deleteByQuery(deleteByQueryRequest,RequestOptions.DEFAULT);
    }


    /**
     * getById
     * @throws IOException
     */
    public static void getById() throws IOException {

        GetRequest getRequest = new GetRequest("movie_2022","1002");
        GetResponse getResponse = esClient.get(getRequest, RequestOptions.DEFAULT);
        Map<String, Object> source = getResponse.getSource();
        System.out.println(source);

    }

    /**
     *
     * @throws IOException
     */
    public static void aggTest() throws IOException{

        SearchRequest searchRequest = new SearchRequest("movie_2022");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        //query
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        boolQueryBuilder.must();
        boolQueryBuilder.filter();
        QueryBuilders.matchQuery("xxx", "xxx");
        //must

        searchRequest.source(searchSourceBuilder);


        esClient.search(searchRequest, RequestOptions.DEFAULT);

    }




}
