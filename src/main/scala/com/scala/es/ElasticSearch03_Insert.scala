package com.scala.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.scala.bean.Movie
import ElasticSearch02_Insert.saveIdempotent
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

/**
 * 批量数据写入
 */
object ElasticSearch03_Insert {
    private val esClient: RestHighLevelClient = ElasticSearch01_ENV.esClient

    def main(args: Array[String]): Unit = {

        val movies = List(
            Movie("102", "速度与激情"),
            Movie("103", "八佰")
        )
        bulkSave(movies, "movie_test20210103")

        esClient.close()
    }

    /**
     * 批量数据写入
     */
    def bulkSave(sourceList: List[AnyRef], indexName: String): Unit = {
        // BulkRequest实际上就是由多个单条IndexRequest的组合
        val bulkRequest = new BulkRequest()

        for (source <- sourceList) {
            val indexRequest = new IndexRequest()
            indexRequest.index(indexName)
            val movieJsonStr: String = JSON.toJSONString(source, new SerializeConfig(true))
            indexRequest.source(movieJsonStr, XContentType.JSON)
            bulkRequest.add(indexRequest)
        }

        esClient.bulk(bulkRequest, RequestOptions.DEFAULT)
    }
}
