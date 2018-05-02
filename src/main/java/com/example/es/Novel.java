package com.example.es;

import lombok.Data;
import org.springframework.data.elasticsearch.annotations.Document;

import java.util.Date;

@Data
@Document(indexName="book",type="novel")
public class Novel {

    private String author;
    private String title;
    private Integer word_count;
    private Date publish_date;
}
