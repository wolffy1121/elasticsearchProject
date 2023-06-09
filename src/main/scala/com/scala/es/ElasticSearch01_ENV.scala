package com.scala.es

import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}

/**
 * ElasticSearch01_ENV:esClient
 */
object ElasticSearch01_ENV {

    //声明es客户端
    var esClient: RestHighLevelClient = build()
    /**
     * 销毁 esClient
     */
    def close(): Unit = {
        esClient.close()
        esClient = null
    }

    /**
     * 创建es客户端对象
     */
    def build(): RestHighLevelClient = {
        val builder: RestClientBuilder = RestClient.builder(new HttpHost("hadoop102", 9200))
        val esClient = new RestHighLevelClient(builder)
        esClient
    }
    def main(args: Array[String]): Unit = {
        println(esClient)
        esClient.close()
    }

}
