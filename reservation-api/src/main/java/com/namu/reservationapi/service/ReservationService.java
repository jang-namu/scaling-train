package com.namu.reservationapi.service;

import com.namu.reservationapi.dto.ReservationRequestDto;
import com.namu.reservationapi.dto.ReservationResponseDto;
import com.namu.reservationapi.entity.Reservation;
import com.namu.reservationapi.repository.ReservationRepository;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;


@Service
public class ReservationService {

    @Autowired
    private ReservationRepository reservationRepository;

    @Autowired
    private KafkaProducerService kafkaProducerService;

    @Transactional
    public ReservationResponseDto createReservation(ReservationRequestDto request) {
        Reservation reservation = new Reservation();
        reservation.setStudentName(request.getStudentName());
        reservation.setStudentEmail(request.getStudentEmail());
        reservation.setStudentPhone(request.getStudentPhone());
        reservation.setClassId(request.getClassId());
        reservation.setInstructorId(request.getInstructorId());
        reservation.setReservationDate(request.getReservationDate());
        reservation.setStatus("PENDING");
        reservation.setCreatedAt(LocalDateTime.now());

        Reservation savedReservation = reservationRepository.save(reservation);
        kafkaProducerService.sendReservationEvent(savedReservation);

        return convertToResponseDto(savedReservation);
    }

    public List getAllReservations() {
        return reservationRepository.findAll().stream()
                .map(this::convertToResponseDto)
                .collect(Collectors.toList());
    }

    public ReservationResponseDto getReservation(Long id) {
        Reservation reservation = reservationRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Reservation not found"));
        return convertToResponseDto(reservation);
    }

    @Transactional
    public void cancelReservation(Long id) {
        Reservation reservation = reservationRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Reservation not found"));

        reservation.setStatus("CANCELLED");
        reservationRepository.save(reservation);
        kafkaProducerService.sendCancellationEvent(reservation);
    }

    private ReservationResponseDto convertToResponseDto(Reservation reservation) {
        ReservationResponseDto dto = new ReservationResponseDto();
        dto.setId(reservation.getId());
        dto.setStudentName(reservation.getStudentName());
        dto.setStudentEmail(reservation.getStudentEmail());
        dto.setClassId(reservation.getClassId());
        dto.setInstructorId(reservation.getInstructorId());
        dto.setReservationDate(reservation.getReservationDate());
        dto.setStatus(reservation.getStatus());
        dto.setCreatedAt(reservation.getCreatedAt());
        return dto;
    }
}