package es

import org.apache.http.HttpHost
import org.elasticsearch.client.{RestClient, RestClientBuilder, RestHighLevelClient}

object EsTest01_insert {
    //声明es客户端
    var esClient : RestHighLevelClient = build()

    def main(args: Array[String]): Unit = {

//        val movie = Movie("100","梅艳芳")
//        save(movie,"movie_test20210103")

        destory()
    }
    /**
     * 销毁
     */
    def destory(): Unit ={
        esClient.close()
        esClient = null
    }

    /**
     * 创建es客户端对象
     */
    def build():RestHighLevelClient = {
        val builder: RestClientBuilder = RestClient.builder(new HttpHost("hadoop102",9200))
        val esClient = new RestHighLevelClient(builder)
        esClient
    }
}
