package com.namu.reservationapi.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.namu.reservationapi.entity.Reservation;
import java.util.HashMap;
import java.util.Map;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;


@Service
public class KafkaProducerService {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String RESERVATION_TOPIC = "reservation-events";

    public void sendReservationEvent(Reservation reservation) {
        try {
            Map event = new HashMap<>();
            event.put("eventType", "RESERVATION_CREATED");
            event.put("reservationId", reservation.getId());
            event.put("studentName", reservation.getStudentName());
            event.put("studentEmail", reservation.getStudentEmail());
            event.put("classId", reservation.getClassId());
            event.put("instructorId", reservation.getInstructorId());
            event.put("reservationDate", reservation.getReservationDate().toString());
            event.put("timestamp", System.currentTimeMillis());

            String eventJson = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(RESERVATION_TOPIC, eventJson);

            System.out.println("Reservation event sent: " + eventJson);
        } catch (JsonProcessingException e) {
            System.err.println("Error sending reservation event: " + e.getMessage());
        }
    }

    public void sendCancellationEvent(Reservation reservation) {
        try {
            Map event = new HashMap<>();
            event.put("eventType", "RESERVATION_CANCELLED");
            event.put("reservationId", reservation.getId());
            event.put("studentEmail", reservation.getStudentEmail());
            event.put("timestamp", System.currentTimeMillis());

            String eventJson = objectMapper.writeValueAsString(event);
            kafkaTemplate.send(RESERVATION_TOPIC, eventJson);

            System.out.println("Cancellation event sent: " + eventJson);
        } catch (JsonProcessingException e) {
            System.err.println("Error sending cancellation event: " + e.getMessage());
        }
    }
}
