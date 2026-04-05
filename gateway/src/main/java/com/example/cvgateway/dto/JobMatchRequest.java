package com.example.cvgateway.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import java.util.List;
import lombok.Data;

@Data
public class JobMatchRequest {
    @NotBlank
    @Size(min = 20)
    private String jobDescription;

    @NotEmpty
    private List<String> requiredSkills;

    @Min(0)
    private int minSeniorityYears = 0;

    @Pattern(regexp = "^$|^[A-Z]{2}$")
    private String location = "";

    @Min(1)
    @Max(50)
    private int topK = 10;
}
