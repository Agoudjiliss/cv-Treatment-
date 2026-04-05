package com.example.cvgateway.config;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.PutMappingRequest;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import java.io.IOException;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchConfig {

    @Bean(destroyMethod = "close")
    public RestClient restClient(@Value("${cv.index.endpoint}") String endpoint) {
        return RestClient.builder(HttpHost.create(endpoint)).build();
    }

    @Bean
    public ElasticsearchTransport elasticsearchTransport(RestClient restClient) {
        return new RestClientTransport(restClient, new JacksonJsonpMapper());
    }

    @Bean
    public ElasticsearchClient elasticsearchClient(
            ElasticsearchTransport transport,
            @Value("${cv.index.index-name}") String indexName) throws IOException {
        ElasticsearchClient client = new ElasticsearchClient(transport);
        boolean exists = client.indices().exists(ExistsRequest.of(r -> r.index(indexName))).value();
        if (!exists) {
            client.indices().create(CreateIndexRequest.of(r -> r.index(indexName)));
            client.indices().putMapping(PutMappingRequest.of(r -> r
                    .index(indexName)
                    .properties("cvId", p -> p.keyword(k -> k))
                    .properties("skillsSet", p -> p.keyword(k -> k))
                    .properties("languages", p -> p.keyword(k -> k))
                    .properties("seniorityYears", p -> p.integer(i -> i))
                    .properties("locationCountry", p -> p.keyword(k -> k))
                    .properties("confidence", p -> p.float_(f -> f))
                    .properties("embeddingVector", p -> p.denseVector(v -> v
                            .dims(384)
                            .index(true)
                            .similarity("cosine")))
                    .properties("cvJson", p -> p.object(o -> o.enabled(true)))
                    .properties("indexedAt", p -> p.date(d -> d))));
        }
        return client;
    }
}
