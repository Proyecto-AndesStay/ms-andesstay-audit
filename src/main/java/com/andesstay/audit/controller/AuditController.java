package com.andesstay.audit.controller;

import com.andesstay.audit.dto.ReservationStreamEvent;
import com.andesstay.audit.service.AuditKafkaConsumerService;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/audit")
@CrossOrigin(origins = "*")
public class AuditController {

    private final AuditKafkaConsumerService auditService;

    public AuditController(AuditKafkaConsumerService auditService) {
        this.auditService = auditService;
    }

    @GetMapping
    public List<ReservationStreamEvent> getAuditEvents() {
        return auditService.getAllAuditLogs();
    }
}