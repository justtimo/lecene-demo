package com.wby.lucene.demo.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Service
@Slf4j
public class LuceneIndexService {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Directory directory;
    private IndexWriter indexWriter;
    private IndexSearcher indexSearcher;
    private IndexReader indexReader;
    private final StandardAnalyzer analyzer = new StandardAnalyzer();

    @PostConstruct
    public void init() {
        try {
            directory = new MMapDirectory(Paths.get("indexDir"));
            IndexWriterConfig config = new IndexWriterConfig(analyzer);
            indexWriter = new IndexWriter(directory, config);
            indexReader = DirectoryReader.open(indexWriter);
            indexSearcher = new IndexSearcher(indexReader);
            loadInitialDocuments();
        } catch (IOException e) {
            log.error("Failed to initialize Lucene index:{}", e);
        }
    }

    @PreDestroy
    public void close() {
        try {
            indexWriter.close();
            directory.close();
        } catch (IOException e) {
            log.error("Failed to close Lucene index: {}", e);
        }
    }

    private void loadInitialDocuments() {
        //将初始文档加载到索引中的实现
    }

    public void updateDocuments(List<Document> documents) {
        lock.writeLock().lock();
        try {
            for (Document doc : documents) {
                indexWriter.updateDocument(new Term("id", doc.get("id")), doc);
            }
            indexWriter.commit();
            refreshIndexSearcher();
        } catch (IOException e) {
            log.error("Failed to update documents:{}", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    public List<Document> searchDocuments(String queryStr, int start, int rows) {
        lock.readLock().lock();
        try {
            QueryParser parser = new QueryParser("title", analyzer);
            Query query = parser.parse(queryStr);
            TopDocs topDocs = indexSearcher.search(query, start + rows);
            List<Document> results = new ArrayList<>();
            for (int i = start; i < Math.min(topDocs.scoreDocs.length, start + rows); i++) {
                results.add(indexSearcher.doc(topDocs.scoreDocs[i].doc));
            }
            return results;
        } catch (Exception e) {
            log.error("Failed to search documents:{}", e);
            return Collections.emptyList();
        } finally {
            lock.readLock().unlock();
        }
    }

    public int countDocuments(Query query) {
        lock.readLock().lock();
        try {
            TotalHitCountCollector collector = new TotalHitCountCollector();
            indexSearcher.search(query, collector);
            return collector.getTotalHits();
        } catch (IOException e) {
            log.error("Failed to count documents:{}", e);
            return 0;
        } finally {
            lock.readLock().unlock();
        }
    }

    private void refreshIndexSearcher() {
        try {
            DirectoryReader newReader = DirectoryReader.openIfChanged((DirectoryReader) indexReader);
            if (newReader != null) {
                indexReader.close();
                indexReader = newReader;
                indexSearcher = new IndexSearcher(indexReader);
            }
        } catch (IOException e) {
            log.error("Failed to refresh IndexSearcher:{}", e);
        }
    }
}
