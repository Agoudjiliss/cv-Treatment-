package com.example.cvgateway.service;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import com.example.cvgateway.dto.JobMatchRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class CvFilterService {

    private final ElasticsearchClient elasticsearchClient;

    @Value("${cv.index.index-name}")
    private String indexName;

    public List<Map<String, Object>> filterCandidates(JobMatchRequest request) {
        try {
            Query query = buildQuery(request);
            SearchResponse<Map> response = elasticsearchClient.search(s -> s
                            .index(indexName)
                            .size(5000)
                            .query(query)
                            .source(src -> src.filter(f -> f.includes("cvId", "embeddingVector", "skillsSet", "cvJson", "confidence"))),
                    Map.class);
            List<Map<String, Object>> hits = new ArrayList<>();
            response.hits().hits().forEach(h -> {
                if (h.source() != null) {
                    hits.add(h.source());
                }
            });
            if (hits.isEmpty() && request.getMinSeniorityYears() > 0) {
                request.setMinSeniorityYears(0);
                return filterCandidates(request);
            }
            return hits;
        } catch (Exception ex) {
            return List.of();
        }
    }

    private Query buildQuery(JobMatchRequest request) {
        BoolQuery.Builder bool = new BoolQuery.Builder();
        bool.must(m -> m.terms(t -> t.field("skillsSet").terms(v -> v.value(
                request.getRequiredSkills().stream().map(s -> co.elastic.clients.elasticsearch._types.FieldValue.of(s)).toList()))));
        bool.filter(f -> f.range(r -> r
                .field("seniorityYears")
                .gte(JsonData.of(request.getMinSeniorityYears()))));
        if (request.getLocation() != null && !request.getLocation().isBlank()) {
            bool.filter(f -> f.term(t -> t.field("locationCountry").value(request.getLocation())));
        }
        return Query.of(q -> q.bool(bool.build()));
    }
}
