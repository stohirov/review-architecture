package com.example.platform.payment.psp;

import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.ClientHttpRequestFactories;
import org.springframework.boot.web.client.ClientHttpRequestFactorySettings;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;

import java.math.BigDecimal;
import java.time.Duration;

/**
 * Captures are idempotent at the PSP by {@code Idempotency-Key}, so the retry and a redelivered
 * ChargePayment never double-charge.
 */
@Component
public class PspClient {

    private final RestClient http;

    public PspClient(@Value("${psp.base-url}") String baseUrl,
                     @Value("${psp.api-key}") String apiKey,
                     @Value("${psp.connect-timeout}") Duration connectTimeout,
                     @Value("${psp.read-timeout}") Duration readTimeout) {
        this.http = RestClient.builder()
                .baseUrl(baseUrl)
                .defaultHeader("Authorization", "Bearer " + apiKey)
                .requestFactory(ClientHttpRequestFactories.get(ClientHttpRequestFactorySettings.DEFAULTS
                        .withConnectTimeout(connectTimeout)
                        .withReadTimeout(readTimeout)))
                .build();
    }

    public record CaptureRequest(String customerId, BigDecimal amount, String currency) {
    }

    public record CaptureResponse(String reference, String outcome) {
        public boolean approved() {
            return "approved".equals(outcome);
        }
    }

    @CircuitBreaker(name = "psp")
    @Retry(name = "psp")
    public CaptureResponse capture(String idempotencyKey, String customerId, BigDecimal amount) {
        return http.post()
                .uri("/v1/captures")
                .header("Idempotency-Key", idempotencyKey)
                .body(new CaptureRequest(customerId, amount, "EUR"))
                .retrieve()
                .body(CaptureResponse.class);
    }
}
