package com.demo.assetservice.kioskmm.api;

import com.demo.assetservice.kioskmm.dto.KioskMmRequestDto;
import com.demo.assetservice.kioskmm.dto.KioskMmResponseDto;
import com.demo.assetservice.kioskmm.services.KioskMmService;
import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kiosk-mms")
public class KioskMmController {

	private final KioskMmService kioskMmService;

	public KioskMmController(KioskMmService kioskMmService) {
		this.kioskMmService = kioskMmService;
	}

	@GetMapping
	public List<KioskMmResponseDto> getAllKioskMms(
		@RequestParam(required = false) Long locationId
	) {
		if (locationId != null) {
			return kioskMmService.getKioskMmsByLocationId(locationId);
		}
		return kioskMmService.getAllKioskMms();
	}

	@GetMapping("/{id}")
	public KioskMmResponseDto getKioskMmById(@PathVariable Long id) {
		return kioskMmService.getKioskMmById(id);
	}

	@PostMapping
	public KioskMmResponseDto createKioskMm(
		@RequestBody KioskMmRequestDto requestDto,
		@RequestParam Long locationId
	) {
		return kioskMmService.createKioskMm(requestDto, locationId);
	}

	@PutMapping("/{id}")
	public KioskMmResponseDto updateKioskMm(
		@PathVariable Long id,
		@RequestBody KioskMmRequestDto requestDto,
		@RequestParam(required = false) Long locationId
	) {
		return kioskMmService.updateKioskMm(id, requestDto, locationId);
	}

	@DeleteMapping("/{id}")
	public void deleteKioskMm(@PathVariable Long id) {
		kioskMmService.deleteKioskMm(id);
	}
}
