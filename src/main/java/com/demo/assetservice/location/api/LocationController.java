package com.demo.assetservice.location.api;

import com.demo.assetservice.location.dto.LocationRequestDto;
import com.demo.assetservice.location.dto.LocationResponseDto;
import com.demo.assetservice.location.services.LocationService;
import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/locations")
public class LocationController {

	private final LocationService locationService;

	public LocationController(LocationService locationService) {
		this.locationService = locationService;
	}

	@GetMapping
	public List<LocationResponseDto> getAllLocations() {
		return locationService.getAllLocations();
	}

	@GetMapping("/{id}")
	public LocationResponseDto getLocationById(@PathVariable Long id) {
		return locationService.getLocationById(id);
	}

	@PostMapping
	public LocationResponseDto createLocation(@RequestBody LocationRequestDto requestDto) {
		return locationService.createLocation(requestDto);
	}

	@PutMapping("/{id}")
	public LocationResponseDto updateLocation(@PathVariable Long id, @RequestBody LocationRequestDto requestDto) {
		return locationService.updateLocation(id, requestDto);
	}

	@DeleteMapping("/{id}")
	public void deleteLocation(@PathVariable Long id) {
		locationService.deleteLocation(id);
	}
}
