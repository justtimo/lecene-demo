package com.wby.lucene.demo.service;

import org.apache.lucene.document.*;
import org.apache.lucene.search.Query;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
public class LuceneIndexServiceTest {

    @Autowired
    private LuceneIndexService luceneIndexService;


    @Test
    public void testUpdateAndSearchDocuments() {
        Document doc1 = new Document();
        doc1.add(new StringField("id", "1", Field.Store.YES));
        doc1.add(new TextField("title", "Lucene introduction", Field.Store.YES));
        doc1.add(new StringField("status", "published", Field.Store.YES));
        doc1.add(new LongPoint("time", 1627849200000L));

        Document doc2 = new Document();
        doc2.add(new StringField("id", "2", Field.Store.YES));
        doc2.add(new TextField("title", "Lucene advanced", Field.Store.YES));
        doc2.add(new StringField("status", "draft", Field.Store.YES));
        doc2.add(new LongPoint("time", 1627935600000L));

        luceneIndexService.updateDocuments(Arrays.asList(doc1, doc2));

        List<Document> results = luceneIndexService.searchDocuments("Lucene", Arrays.asList("published", "draft"), 1627849200000L, 1627935600000L, 0, 10);
        assertEquals(2, results.size());

        int count = luceneIndexService.countDocuments("Lucene", Arrays.asList("published", "draft"), 1627849200000L, 1627935600000L);
        assertEquals(2, count);
    }
}

