package com.andesstay.audit.service;

import com.andesstay.audit.dto.ReservationStreamEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AuditKafkaConsumerServiceTest {

    private AuditKafkaConsumerService service;

    @BeforeEach
    void setUp() {
        service = new AuditKafkaConsumerService();
    }

    @Test
    void consumeAuditEvent_DeberiaGuardarEventosConElMasRecientePrimero() {
        service.consumeAuditEvent("{\"eventType\":\"RESERVATION_CREATED\",\"reservationId\":1}", 0, 1L);
        service.consumeAuditEvent("{\"eventType\":\"STATUS_UPDATED\",\"reservationId\":2}", 0, 2L);

        List<ReservationStreamEvent> logs = service.getAllAuditLogs();

        assertEquals(2, logs.size());
        assertEquals("STATUS_UPDATED", logs.get(0).getEventType());
        assertEquals(2L, logs.get(0).getReservationId());
        assertEquals("RESERVATION_CREATED", logs.get(1).getEventType());
    }

    @Test
    void consumeAuditEvent_DeberiaDescartarMensajesVaciosOSinEventType() {
        service.consumeAuditEvent(null, 0, 1L);
        service.consumeAuditEvent("   ", 0, 2L);
        service.consumeAuditEvent("{\"reservationId\":5}", 1, 3L);

        assertTrue(service.getAllAuditLogs().isEmpty());
    }

    @Test
    void consumeAuditEvent_NoDeberiaLanzarExcepcionConJsonInvalido() {
        assertDoesNotThrow(() -> service.consumeAuditEvent("{json-invalido", 0, 10L));
        assertTrue(service.getAllAuditLogs().isEmpty());
    }
}