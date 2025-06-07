package com.namu.reservationapi.controller;

import com.namu.reservationapi.dto.ReservationRequestDto;
import com.namu.reservationapi.dto.ReservationResponseDto;
import com.namu.reservationapi.service.ReservationService;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/reservations")
@CrossOrigin(origins = "*")
public class ReservationController {

    @Autowired
    private ReservationService reservationService;

    @PostMapping
    public ResponseEntity createReservation(
            @RequestBody ReservationRequestDto request) {
        ReservationResponseDto response = reservationService.createReservation(request);
        return ResponseEntity.ok(response);
    }

    @GetMapping
    public ResponseEntity<List> getAllReservations() {
        List reservations = reservationService.getAllReservations();
        return ResponseEntity.ok(reservations);
    }

    @GetMapping("/{id}")
    public ResponseEntity getReservation(@PathVariable Long id) {
        ReservationResponseDto reservation = reservationService.getReservation(id);
        return ResponseEntity.ok(reservation);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity cancelReservation(@PathVariable Long id) {
        reservationService.cancelReservation(id);
        return ResponseEntity.noContent().build();
    }

    @GetMapping("/health")
    public ResponseEntity health() {
        return ResponseEntity.ok("API Server is running!");
    }
}