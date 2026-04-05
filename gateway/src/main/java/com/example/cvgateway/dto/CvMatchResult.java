package com.example.cvgateway.dto;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class CvMatchResult {
    private int rank;
    private String cvId;
    private double vectorScore;
    private double confidence;
    private List<String> matchedSkills;
    private List<String> missingSkills;
    private String explanation;
    private JsonNode contact;
}
