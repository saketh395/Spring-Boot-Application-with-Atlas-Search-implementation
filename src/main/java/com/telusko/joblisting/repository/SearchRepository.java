package com.telusko.joblisting.repository;

import com.telusko.joblisting.model.Post;

import java.util.List;

public interface SearchRepository {

    List<Post> findByText(String tech, String profile);

    int count(String tech, String profile);

}
