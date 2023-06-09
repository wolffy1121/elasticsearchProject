package com.scala.es

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import com.scala.utils.MyElasticSerachUtils
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.xcontent.XContentType

/**
 * GET /movie_test20210103/_search
 */
object ElasticSearch05_Update {

    private val esClient: RestHighLevelClient = MyElasticSerachUtils.esClient

    def main(args: Array[String]): Unit = {

        val source: (String, String, String) = ("101", "movie_name", "功夫功夫功夫2222")
        update(source, "movie_test20210103")

        esClient.close()
    }

    /**
     * 根据 docid 更新字段
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
