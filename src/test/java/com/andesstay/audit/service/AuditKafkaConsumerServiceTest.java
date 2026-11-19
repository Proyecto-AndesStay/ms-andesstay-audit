package com.andesstay.audit.service;

import com.andesstay.audit.dto.ReservationStreamEvent;
import com.andesstay.audit.entity.AuditLogEntity;
import com.andesstay.audit.repository.AuditLogRepository;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class AuditKafkaConsumerServiceTest {

    private AuditLogRepository inMemoryRepository() {
        List<AuditLogEntity> store = new ArrayList<>();
        AtomicLong seq = new AtomicLong(1);
        return (AuditLogRepository) Proxy.newProxyInstance(
                AuditLogRepository.class.getClassLoader(),
                new Class<?>[]{AuditLogRepository.class},
                (proxy, method, args) -> {
                    String name = method.getName();
                    if ("save".equals(name) && args != null && args.length == 1) {
                        AuditLogEntity entity = (AuditLogEntity) args[0];
                        if (entity.getId() == null) {
                            entity.setId(seq.getAndIncrement());
                        }
                        store.removeIf(e -> e.getId().equals(entity.getId()));
                        store.add(entity);
                        return entity;
                    }
                    if ("findAllByOrderByIdDesc".equals(name)) {
                        return store.stream()
                                .sorted(Comparator.comparing(AuditLogEntity::getId).reversed())
                                .toList();
                    }
                    if ("findAll".equals(name)) {
                        return new ArrayList<>(store);
                    }
                    if ("count".equals(name)) {
                        return (long) store.size();
                    }
                    if ("deleteAll".equals(name)) {
                        store.clear();
                        return null;
                    }
                    return null;
                }
        );
    }

    @Test
    void shouldConsumeAndStoreAuditEvent() {
        AuditKafkaConsumerService service = new AuditKafkaConsumerService(inMemoryRepository());
        String payload = """
                {
                  "eventId": "evt-1",
                  "eventType": "RESERVATION_CREATED",
                  "reservationId": 100,
                  "guestName": "Test Guest",
                  "roomType": "DOUBLE",
                  "status": "CONFIRMED",
                  "performedBy": "system",
                  "timestamp": "2026-11-19T08:00:00Z",
                  "details": "Created in test"
                }
                """;

        service.consumeAuditEvent(payload, 0, 1L);

        List<ReservationStreamEvent> logs = service.getAllAuditLogs();
        assertNotNull(logs);
        assertEquals(1, logs.size());
        assertEquals("RESERVATION_CREATED", logs.get(0).getEventType());
        assertEquals(100L, logs.get(0).getReservationId());
    }
}
