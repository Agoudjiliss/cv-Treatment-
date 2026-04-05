package com.example.cvgateway.exception;

public class CvScoringException extends RuntimeException {

    public CvScoringException(String message) {
        super(message);
    }

    public CvScoringException(String message, Throwable cause) {
        super(message, cause);
    }
}
