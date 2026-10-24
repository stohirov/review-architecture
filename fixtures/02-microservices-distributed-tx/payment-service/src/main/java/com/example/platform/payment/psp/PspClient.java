package com.example.platform.payment.psp;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;

@Component
public class PspClient {

    private final RestTemplate restTemplate;
    private final String baseUrl;
    private final String apiKey;

    public PspClient(RestTemplate restTemplate, @Value("${psp.base-url}") String baseUrl,
                     @Value("${psp.api-key}") String apiKey) {
        this.restTemplate = restTemplate;
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
    }

    public record CaptureRequest(String customerId, BigDecimal amount, String currency) {
    }

    public record CaptureResponse(String reference, String outcome) {
    }

    public CaptureResponse capture(String customerId, BigDecimal amount) {
        HttpHeaders headers = new HttpHeaders();
        headers.setBearerAuth(apiKey);
        return restTemplate.postForObject(baseUrl + "/v1/captures",
                new HttpEntity<>(new CaptureRequest(customerId, amount, "EUR"), headers),
                CaptureResponse.class);
    }
}
