package com.wolffy.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.wolffy.bean.Movie
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

/**
 * GET /movie_test20210103/_search
 */
object ElasticSearch02_Insert {

    private val esClient: RestHighLevelClient = ElasticSearch01_ENV.esClient

    def main(args: Array[String]): Unit = {

        val source: (String, Movie) = ("101",Movie("101","功夫"))

        saveIdempotent(source,"movie_test20210103")
        esClient.close()
    }

    /**
     * 单条数据幂等写入
     * 通过指定id实现幂等
     */
    def saveIdempotent(source: (String, AnyRef), indexName: String): Unit = {
        val indexRequest = new IndexRequest()
        indexRequest.index(indexName)
        val movieJsonStr: String = JSON.toJSONString(source._2, new SerializeConfig(true))

        indexRequest.source(movieJsonStr, XContentType.JSON)
        indexRequest.id(source._1)
        esClient.index(indexRequest, RequestOptions.DEFAULT)
    }
}
