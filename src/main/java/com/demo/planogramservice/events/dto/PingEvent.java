package com.demo.planogramservice.events.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PingEvent {
    private String id;
    private String message;
    private String sourceService;
    private Instant timestamp;
}
