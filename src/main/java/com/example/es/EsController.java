package com.example.es;


import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutionException;

@RestController
public class EsController {

    @Autowired
    private TransportClient client;

    @GetMapping("/get/book/novel")
    public ResponseEntity get(@RequestParam(value = "id", defaultValue = "") String id) {
        if (id.isEmpty()) {
            return new ResponseEntity(HttpStatus.NOT_FOUND);
        }

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


}
