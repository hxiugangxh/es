package com.example.es;


import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.SearchQuery;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;

@RestController
public class EsController {

    @Autowired
    private TransportClient client;

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    @GetMapping("/get/book/novel")
    public ResponseEntity get(@RequestParam(value = "id", defaultValue = "") String id) {
        if (id.isEmpty()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        SearchQuery searchQuery = new NativeSearchQueryBuilder().withQuery(queryStringQuery("大发")).build();
        List<Novel> articles = elasticsearchTemplate.queryForList(searchQuery, Novel.class);
        for (Novel article : articles) {
            System.out.println(article);
        }

        System.out.println("elasticsearchTemplate = " + elasticsearchTemplate);

        GetResponse result = this.client.prepareGet("book", "novel", id).get();

        if (!result.isExists()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

        return new ResponseEntity(result.getSource(), HttpStatus.OK);
    }

    @PostMapping("/add/book/novel")
    public ResponseEntity add(
            @RequestParam("title") String title,
            @RequestParam("author") String author,
            @RequestParam("wordCount") String wordCount,
            @RequestParam("publishDate")
            @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
                    Date publishDate
    ) {
        try {
            XContentBuilder content = XContentFactory.jsonBuilder()
                    .startObject()
                    .field("title", title)
                    .field("author", author)
                    .field("wordCount", wordCount)
                    .field("publishDate", publishDate)
                    .endObject();

            IndexResponse result = this.client.prepareIndex("index", "novel")
                    .setSource(content)
                    .get();

            return new ResponseEntity(result.getId(), HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @DeleteMapping("/del/book/novel")
    public ResponseEntity del(@RequestParam("id") String id) {
        DeleteResponse deleteResponse = this.client.prepareDelete("book", "novel", id).get();

        return new ResponseEntity(deleteResponse.getResult().toString(), HttpStatus.OK);
    }

    @PutMapping("/update/book/novel")
    public ResponseEntity update(
            @RequestParam(value = "id", required = false) String id,
            @RequestParam("title") String title,
            @RequestParam("author") String author
    ) {
        UpdateRequest updateRequest = new UpdateRequest("book", "novel", id);

        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().startObject();

            if (null != title) {
                builder.field("title", title);
            }
            if (null != author) {
                builder.field("author", author);
            }

            builder.endObject();
            updateRequest.doc(builder);

            UpdateResponse updateResponse = this.client.update(updateRequest).get();

            return new ResponseEntity(updateResponse.getResult().toString(), HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();

            return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    @PostMapping("/test")
    public ResponseEntity test() throws Exception {

        MatchPhraseQueryBuilder matchPhraseQueryBuilder = QueryBuilders.matchPhraseQuery("title", "Elasticsearch入门");

        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery().must(matchPhraseQueryBuilder);

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(boolQueryBuilder);
        System.out.println(sourceBuilder.toString());

        SearchResponse searchResponse = this.client.prepareSearch("book").setTypes("novel")
                .setQuery(boolQueryBuilder).setExplain(true).execute().actionGet();

        SearchHits hits = searchResponse.getHits();

        List<Map<String, Object>> result = new ArrayList<>();
        for (SearchHit hit : hits) {
            result.add(hit.getSource());
        }

        return new ResponseEntity(result, HttpStatus.OK);
    }

    @PostMapping("/query/book/novel")
    public ResponseEntity query(
            @RequestParam(value = "author", required = false) String author,
            @RequestParam(value = "title", required = false) String title,
            @RequestParam(value = "gtWordCount", defaultValue = "10") Integer gtWordCount
    ) {
        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();

        if (author != null) {
            boolQuery.must(QueryBuilders.matchQuery("author", author));
        }
        if (title != null) {
            boolQuery.must(QueryBuilders.matchQuery("title", title));
        }

        RangeQueryBuilder rangeQuery = QueryBuilders.rangeQuery("word_count")
                .from(gtWordCount).to(5000);

        boolQuery.filter(rangeQuery);

        SearchRequestBuilder searchRequestBuilder = this.client.prepareSearch("book")
                .setTypes("novel")
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .setQuery(boolQuery)
                .setFrom(0)
                .setSize(10);

        System.out.println(searchRequestBuilder);

        SearchResponse searchResponse = searchRequestBuilder.get();
        SearchHits hits = searchResponse.getHits();

        List<Map<String, Object>> result = new ArrayList<>();

        for (SearchHit hit : hits) {
            result.add(hit.getSource());
        }

        return new ResponseEntity(result, HttpStatus.OK);
    }

}
