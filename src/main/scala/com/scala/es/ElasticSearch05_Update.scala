package com.scala.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

object ElasticSearch05_Update {

    private val esClient: RestHighLevelClient = ElasticSearch01_ENV.esClient

    def main(args: Array[String]): Unit = {

        val source: (String, String, String) = ("101", "movie_name", "功夫功夫功夫")
        update(source, "movie_test20210103")

        esClient.close()
    }

    /**
     * 根据docid更新字段
     */
    def update(source: (String, String, String), indexName: String): Unit = {
        val updateRequest = new UpdateRequest()
        updateRequest.index(indexName)
        JSON.toJSONString(source._2, new SerializeConfig(true))
        updateRequest.id(source._1)
        updateRequest.doc(XContentType.JSON, source._2, source._3)
        esClient.update(updateRequest, RequestOptions.DEFAULT)
    }


}
