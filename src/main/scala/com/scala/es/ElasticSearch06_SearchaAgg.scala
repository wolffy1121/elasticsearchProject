package com.scala.es

import com.scala.es.ElasticSearch06_Search.search
import com.scala.utils.MyElasticSerachUtils
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.search.aggregations.{AggregationBuilders, Aggregations, BucketOrder}
import org.elasticsearch.search.aggregations.bucket.terms.{ParsedStringTerms, Terms, TermsAggregationBuilder}
import org.elasticsearch.search.aggregations.metrics.{AvgAggregationBuilder, ParsedAvg}
import org.elasticsearch.search.builder.SearchSourceBuilder

import java.util
/**
 * 查询每位演员参演的电影的平均分，倒叙排序
 */
object ElasticSearch06_SearchaAgg {

    private val esClient: RestHighLevelClient = MyElasticSerachUtils.esClient

    def main(args: Array[String]): Unit = {

        searchAgg()

        esClient.close()
    }

    /**
     * 查询每位演员参演的电影的平均分，倒叙排序
     */
    def searchAgg(): Unit = {

        val searchRequest = new SearchRequest()
        searchRequest.indices("movie_index")
        val searchSourceBuilder = new SearchSourceBuilder()
        //聚合
        //分组
        val termsAggregationBuilder: TermsAggregationBuilder = AggregationBuilders.
          terms("groupByActor").
          field("actorList.name.keyword").
          size(100).
          order(BucketOrder.aggregation("avg_score", false))
        //avg
        val avgAggregationBuilder: AvgAggregationBuilder = AggregationBuilders.
          avg("avg_score").
          field("doubanScore")

        termsAggregationBuilder.subAggregation(avgAggregationBuilder)
        searchSourceBuilder.aggregation(termsAggregationBuilder)

        searchRequest.source(searchSourceBuilder)
        val searchResponse: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)
        //处理结果
        val groupByActorTerms: ParsedStringTerms = searchResponse.getAggregations.get[ParsedStringTerms]("groupByActor")
        val buckets: util.List[_ <: Terms.Bucket] = groupByActorTerms.getBuckets
        import scala.collection.JavaConverters._
        for (bucket <- buckets.asScala) {
            val actorName: AnyRef = bucket.getKey
            val movieCount: Long = bucket.getDocCount
            val aggregations: Aggregations = bucket.getAggregations
            val parsedAvg: ParsedAvg = aggregations.get[ParsedAvg]("avg_score")
            val avgScore: Double = parsedAvg.getValue
            println(s"$actorName 共参演了 $movieCount 部电影， 平均评分为 $avgScore")

        }
    }
}
