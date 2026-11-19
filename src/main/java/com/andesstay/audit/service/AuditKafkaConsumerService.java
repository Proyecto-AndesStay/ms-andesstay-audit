package com.andesstay.audit.service;

import com.andesstay.audit.dto.ReservationStreamEvent;
import com.andesstay.audit.entity.AuditLogEntity;
import com.andesstay.audit.repository.AuditLogRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AuditKafkaConsumerService {

    private static final Logger log = LoggerFactory.getLogger(AuditKafkaConsumerService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final AuditLogRepository auditLogRepository;

    public AuditKafkaConsumerService(AuditLogRepository auditLogRepository) {
        this.auditLogRepository = auditLogRepository;
    }

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

            AuditLogEntity entity = toEntity(event);
            auditLogRepository.save(entity);

            log.info("[AUDIT-CONSUMER] Evento guardado en MySQL | ID: {} | Partition: {} | Offset: {} | Tipo: {} | Reserva ID: {}",
                    entity.getId(), partition, offset, event.getEventType(), event.getReservationId());

        } catch (Exception e) {
            log.error("[AUDIT-ERROR] Error al procesar mensaje en particion {} offset {}: {}. Payload: {}",
                    partition, offset, e.getMessage(), rawMessage);
        }
    }

    public List<ReservationStreamEvent> getAllAuditLogs() {
        return auditLogRepository.findAllByOrderByIdDesc()
                .stream()
                .map(this::toDto)
                .toList();
    }

    private AuditLogEntity toEntity(ReservationStreamEvent event) {
        AuditLogEntity entity = new AuditLogEntity();
        entity.setEventId(event.getEventId());
        entity.setEventType(event.getEventType());
        entity.setReservationId(event.getReservationId());
        entity.setGuestName(event.getGuestName());
        entity.setRoomType(event.getRoomType());
        entity.setStatus(event.getStatus());
        entity.setPerformedBy(event.getPerformedBy());
        entity.setTimestamp(event.getTimestamp());
        entity.setDetails(event.getDetails());
        return entity;
    }

    private ReservationStreamEvent toDto(AuditLogEntity entity) {
        ReservationStreamEvent dto = new ReservationStreamEvent();
        dto.setEventId(entity.getEventId());
        dto.setEventType(entity.getEventType());
        dto.setReservationId(entity.getReservationId());
        dto.setGuestName(entity.getGuestName());
        dto.setRoomType(entity.getRoomType());
        dto.setStatus(entity.getStatus());
        dto.setPerformedBy(entity.getPerformedBy());
        dto.setTimestamp(entity.getTimestamp());
        dto.setDetails(entity.getDetails());
        return dto;
    }
}
