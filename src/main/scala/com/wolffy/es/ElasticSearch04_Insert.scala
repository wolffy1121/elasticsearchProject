package com.wolffy.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.wolffy.bean.Movie
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

/**
 * 批量数据幂等写入
 */
object ElasticSearch04_Insert {

    private val esClient: RestHighLevelClient = ElasticSearch01_ENV.esClient

    def main(args: Array[String]): Unit = {

        val movies = List(
            ("104" , Movie("104","红海行动")),
            ("105", Movie("105","湄公河行动"))
        )
        bulkSaveIdempotent(movies,"movie_test20210103")

        esClient.close()
    }

    /**
     * 批量数据幂等写入
     * 通过指定id实现幂等
     */
    def bulkSaveIdempotent(sourceList: List[(String, AnyRef)], indexName: String): Unit = {

        // BulkRequest实际上就是由多个单条IndexRequest的组合
        val bulkRequest = new BulkRequest()

        for ((docId, sourceObj) <- sourceList) {
            val indexRequest = new IndexRequest()
            indexRequest.index(indexName)
            val movieJsonStr: String = JSON.toJSONString(sourceObj, new SerializeConfig(true))
            indexRequest.source(movieJsonStr, XContentType.JSON)
            indexRequest.id(docId)
            bulkRequest.add(indexRequest)
        }

        esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
    }

}
