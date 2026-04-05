package com.example.cvgateway.dto;

import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class JobMatchResponse {
    private String jobId;
    private long processingMs;
    private int totalFiltered;
    private List<CvMatchResult> results;
}
