package com.scala.es

import com.scala.es.ElasticSearch05_Update.update
import com.scala.utils.MyElasticSerachUtils
import org.elasticsearch.action.search.{SearchRequest, SearchResponse}
import org.elasticsearch.client.{RequestOptions, RestHighLevelClient}
import org.elasticsearch.common.text.Text
import org.elasticsearch.index.query.{BoolQueryBuilder, MatchQueryBuilder, RangeQueryBuilder}
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.fetch.subphase.highlight.{HighlightBuilder, HighlightField}
import org.elasticsearch.search.sort.SortOrder

import java.util

/**
 * search :
 *
 * 查询doubanScore>=5.0 关键词搜索red sea
 * 关键词高亮显示
 * 显示第一页，每页20条
 * 按doubanScore从大到小排序
 */
object ElasticSearch06_Search {

    private val esClient: RestHighLevelClient = MyElasticSerachUtils.esClient

    def main(args: Array[String]): Unit = {

        search()

        esClient.close()
    }

    def search(): Unit = {
        val searchRequest = new SearchRequest()
        searchRequest.indices("movie_index")
        val searchSourceBuilder = new SearchSourceBuilder()
        //过滤匹配
        val boolQueryBuilder = new BoolQueryBuilder()
        val rangeQueryBuilder = new RangeQueryBuilder("doubanScore").gte("5.0")
        boolQueryBuilder.filter(rangeQueryBuilder)
        val matchQueryBuilder = new MatchQueryBuilder("name", "red sea")
        boolQueryBuilder.must(matchQueryBuilder)
        searchSourceBuilder.query(boolQueryBuilder)
        //高亮
        val highlightBuilder: HighlightBuilder = new HighlightBuilder().field("name")
        searchSourceBuilder.highlighter(highlightBuilder)
        //分页
        searchSourceBuilder.from(0)
        searchSourceBuilder.size(20)
        //排序
        searchSourceBuilder.sort("doubanScore", SortOrder.DESC)

        searchRequest.source(searchSourceBuilder)
        val searchResponse: SearchResponse = esClient.search(searchRequest, RequestOptions.DEFAULT)

        //处理结果
        //总数:
        val total: Long = searchResponse.getHits.getTotalHits.value
        println(s"共查询到 $total 条数据")
        //明细
        val hits: Array[SearchHit] = searchResponse.getHits.getHits
        for (searchHit <- hits) {
            val sourceJson: String = searchHit.getSourceAsString
            //高亮
            val fields: util.Map[String, HighlightField] = searchHit.getHighlightFields
            val field: HighlightField = fields.get("name")
            val fragments: Array[Text] = field.getFragments
            val highlightText: Text = fragments(0)

            println(sourceJson)
            println(highlightText)
        }
    }

}
