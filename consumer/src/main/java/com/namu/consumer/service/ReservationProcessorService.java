package com.namu.consumer.service;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class ReservationProcessorService {

    @Autowired
    private ObjectMapper objectMapper;

    @KafkaListener(topics = "reservation-events", groupId = "reservation-processor")
    public void processReservationEvent(String eventJson) {
        try {
            JsonNode event = objectMapper.readTree(eventJson);
            String eventType = event.get("eventType").asText();

            System.out.println("=== Kafka 이벤트 수신 ===");
            System.out.println("이벤트 타입: " + eventType);
            System.out.println("이벤트 내용: " + eventJson);

            switch (eventType) {
                case "RESERVATION_CREATED":
                    handleReservationCreated(event);
                    break;
                case "RESERVATION_CANCELLED":
                    handleReservationCancelled(event);
                    break;
                default:
                    System.out.println("알 수 없는 이벤트 타입: " + eventType);
            }

        } catch (Exception e) {
            System.err.println("이벤트 처리 중 오류 발생: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private void handleReservationCreated(JsonNode event) {
        String studentName = event.get("studentName").asText();
        String studentEmail = event.get("studentEmail").asText();
        Long reservationId = event.get("reservationId").asLong();

        System.out.println("📧 예약 완료 알림 처리");
        System.out.println("예약자: " + studentName + " (" + studentEmail + ")");
        System.out.println("예약 ID: " + reservationId);
        System.out.println("✅ 예약 생성 이벤트 처리 완료");
    }

    private void handleReservationCancelled(JsonNode event) {
        String studentEmail = event.get("studentEmail").asText();
        Long reservationId = event.get("reservationId").asLong();

        System.out.println("📧 예약 취소 알림 처리");
        System.out.println("이메일: " + studentEmail);
        System.out.println("취소된 예약 ID: " + reservationId);
        System.out.println("✅ 예약 취소 이벤트 처리 완료");
    }
}