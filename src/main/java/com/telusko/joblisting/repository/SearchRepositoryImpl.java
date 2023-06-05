package com.telusko.joblisting.repository;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.telusko.joblisting.model.Post;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


@Component
public class SearchRepositoryImpl implements SearchRepository{

    @Autowired
    MongoClient client;

    @Autowired
    MongoConverter converter;

    @Override
    public List<Post> findByText(String tech, String profile) {

        final List<Post> posts = new ArrayList<>();

        MongoDatabase database = client.getDatabase("Job");
        MongoCollection<Document> collection = database.getCollection("JobPost");
        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(new Document("$search",
                        new Document("index", "default")
                                .append("compound",
                                        new Document("must", Arrays.asList(new Document("text",
                                                        new Document("query", tech)
                                                                .append("path", "techs")),
                                                new Document("text",
                                                        new Document("query", profile)
                                                                .append("path", "profile"))
                                        )))),
                new Document("$sort",
                        new Document("exp", 1L))));

        result.forEach(doc -> posts.add(converter.read(Post.class,doc)));

        return posts;
    }


    public int count(String tech, String profile){

        final List<Post> posts = new ArrayList<>();

        MongoDatabase database = client.getDatabase("Job");
        MongoCollection<Document> collection = database.getCollection("JobPost");
        AggregateIterable<Document> result = collection.aggregate(Arrays.asList(new Document("$search",
                        new Document("index", "default")
                                .append("compound",
                                        new Document("must", Arrays.asList(new Document("text",
                                                        new Document("query", tech)
                                                                .append("path", "techs")),
                                                new Document("text",
                                                        new Document("query", profile)
                                                                .append("path", "profile"))
                                        )))),
                new Document("$sort",
                        new Document("exp", 1L)),
                new Document("$count", "profile")));
        result.forEach(doc -> posts.add(converter.read(Post.class,doc)));

        return Integer.parseInt(posts.get(0).getProfile());
    }


}
