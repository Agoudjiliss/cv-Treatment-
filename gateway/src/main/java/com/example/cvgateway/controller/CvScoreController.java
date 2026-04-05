package com.example.cvgateway.controller;

import com.example.cvgateway.dto.CvScoreRequestDto;
import com.example.cvgateway.dto.ScoredCvDto;
import com.example.cvgateway.service.CvScoringService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/cv")
@RequiredArgsConstructor
public class CvScoreController {

    private final CvScoringService cvScoringService;

    @PostMapping(value = "/score", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<ScoredCvDto>> score(@Valid @RequestBody CvScoreRequestDto request) {
        List<ScoredCvDto> top = cvScoringService.scoreTopMatches(
                request.getJobDescription(),
                request.getParsedCvs());
        return ResponseEntity.ok(top);
    }
}
