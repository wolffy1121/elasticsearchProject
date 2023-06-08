package com.wolffy.es;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;

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
        esClient.close();
    }
}
