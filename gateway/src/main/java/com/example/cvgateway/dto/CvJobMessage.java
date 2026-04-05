package com.example.cvgateway.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CvJobMessage {

    @JsonProperty("correlationId")
    private String correlationId;

    @JsonProperty("filename")
    private String filename;

    @JsonProperty("pdfBase64")
    private String pdfBase64;
}
