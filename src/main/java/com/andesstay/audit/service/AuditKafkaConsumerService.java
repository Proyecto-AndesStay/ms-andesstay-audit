package com.andesstay.audit.service;

import com.andesstay.audit.dto.ReservationStreamEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class AuditKafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(AuditKafkaConsumerService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final List<ReservationStreamEvent> auditHistory = new CopyOnWriteArrayList<>();

    @KafkaListener(topics = "andesstay-auditoria-eventos", groupId = "cg-andesstay-auditoria")
    public void consumeAuditEvent(
            @Payload String rawMessage,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {
        try {
            if (rawMessage == null || rawMessage.isBlank()) {
                log.warn("[AUDIT-WARN] Mensaje vacio descartado en particion {} offset {}", partition, offset);
                return;
            }

            ReservationStreamEvent event = objectMapper.readValue(rawMessage, ReservationStreamEvent.class);

            if (event.getEventType() == null) {
                log.warn("[AUDIT-WARN] Evento sin eventType descartado: {}", rawMessage);
                return;
            }

            auditHistory.add(0, event); // Agrega al inicio para ver el mas reciente primero
            log.info("[AUDIT-CONSUMER] Evento procesado | Partition: {} | Offset: {} | Tipo: {} | Reserva ID: {}",
                    partition, offset, event.getEventType(), event.getReservationId());

        } catch (Exception e) {
            log.error("[AUDIT-ERROR] Error al procesar mensaje en particion {} offset {}: {}. Payload: {}",
                    partition, offset, e.getMessage(), rawMessage);
        }
    }

    public List<ReservationStreamEvent> getAllAuditLogs() {
        return new ArrayList<>(auditHistory);
    }
}