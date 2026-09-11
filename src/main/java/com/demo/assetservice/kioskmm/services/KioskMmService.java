package com.demo.assetservice.kioskmm.services;

import com.demo.assetservice.kioskmm.domain.KioskMm;
import com.demo.assetservice.kioskmm.domain.KioskMmRepository;
import com.demo.assetservice.kioskmm.dto.KioskMmRequestDto;
import com.demo.assetservice.kioskmm.dto.KioskMmResponseDto;
import com.demo.assetservice.location.domain.Location;
import com.demo.assetservice.location.domain.LocationRepository;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class KioskMmService {

	private final KioskMmRepository kioskMmRepository;
	private final LocationRepository locationRepository;

	public KioskMmService(
		KioskMmRepository kioskMmRepository,
		LocationRepository locationRepository
	) {
		this.kioskMmRepository = kioskMmRepository;
		this.locationRepository = locationRepository;
	}

	public List<KioskMmResponseDto> getAllKioskMms() {
		return kioskMmRepository.findAll().stream().map(this::toResponseDto).toList();
	}

	public KioskMmResponseDto getKioskMmById(Long id) {
		KioskMm kioskMm = kioskMmRepository.findById(id)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Kiosk micro-market not found"));
		return toResponseDto(kioskMm);
	}

	public List<KioskMmResponseDto> getKioskMmsByLocationId(Long locationId) {
		return kioskMmRepository.findByLocationId(locationId).stream().map(this::toResponseDto).toList();
	}

	public KioskMmResponseDto createKioskMm(KioskMmRequestDto requestDto, Long locationId) {
		Location location = locationRepository.findById(locationId)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Location not found"));

		KioskMm kioskMm = new KioskMm();
		kioskMm.setKioskCode(requestDto.getKioskCode());
		kioskMm.setModel(requestDto.getModel());
		kioskMm.setStatus(requestDto.getStatus());
		kioskMm.setLocation(location);
		KioskMm savedKioskMm = kioskMmRepository.save(kioskMm);
		return toResponseDto(savedKioskMm);
	}

	public KioskMmResponseDto updateKioskMm(Long id, KioskMmRequestDto requestDto, Long locationId) {
		KioskMm existingKioskMm = findKioskMmByIdOrThrow(id);

		existingKioskMm.setKioskCode(requestDto.getKioskCode());
		existingKioskMm.setModel(requestDto.getModel());
		existingKioskMm.setStatus(requestDto.getStatus());

		if (locationId != null) {
			Location location = locationRepository.findById(locationId)
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Location not found"));
			existingKioskMm.setLocation(location);
		}

		KioskMm savedKioskMm = kioskMmRepository.save(existingKioskMm);
		return toResponseDto(savedKioskMm);
	}

	public void deleteKioskMm(Long id) {
		KioskMm kioskMm = findKioskMmByIdOrThrow(id);
		kioskMmRepository.delete(kioskMm);
	}

	private KioskMm findKioskMmByIdOrThrow(Long id) {
		return kioskMmRepository.findById(id)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Kiosk micro-market not found"));
	}

	private KioskMmResponseDto toResponseDto(KioskMm kioskMm) {
		Long locationId = kioskMm.getLocation() != null ? kioskMm.getLocation().getId() : null;

		return KioskMmResponseDto.builder()
			.id(kioskMm.getId())
			.kioskCode(kioskMm.getKioskCode())
			.locationName(kioskMm.getLocation().getName())
			.model(kioskMm.getModel())
			.status(kioskMm.getStatus())
			.locationId(locationId)
			.build();
	}
}
