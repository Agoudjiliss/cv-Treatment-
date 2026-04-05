package com.example.cvgateway.controller;

import com.example.cvgateway.dto.JobMatchRequest;
import com.example.cvgateway.dto.JobMatchResponse;
import com.example.cvgateway.service.CvMatchService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/v1/jobs")
@RequiredArgsConstructor
public class JobMatchController {

    private final CvMatchService cvMatchService;

    @PostMapping(value = "/match", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<JobMatchResponse> match(@Valid @RequestBody JobMatchRequest request) {
        return ResponseEntity.ok(cvMatchService.match(request));
    }
}
