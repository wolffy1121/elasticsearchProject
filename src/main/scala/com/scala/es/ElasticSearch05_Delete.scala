package com.scala.es

import com.scala.utils.MyElasticSerachUtils
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}

/**
 * Delete
 * GET /movie_test20210103/_search
 */
object ElasticSearch05_Delete {

    private val esClient: RestHighLevelClient = MyElasticSerachUtils.esClient

    def main(args: Array[String]): Unit = {
        deleteById("101","movie_test20210103")
        esClient.close()
    }
    /**
     * 根据ID删除
     */
    def deleteById(docid : String , indexName : String ): Unit ={
        val deleteRequest = new DeleteRequest()
        deleteRequest.index(indexName)
        deleteRequest.id(docid)
        esClient.delete(deleteRequest,RequestOptions.DEFAULT)
    }


}
