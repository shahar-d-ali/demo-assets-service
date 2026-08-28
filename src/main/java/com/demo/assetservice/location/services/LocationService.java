package com.demo.assetservice.location.services;

import com.demo.assetservice.location.domain.Location;
import com.demo.assetservice.location.domain.LocationRepository;
import com.demo.assetservice.location.dto.LocationRequestDto;
import com.demo.assetservice.location.dto.LocationResponseDto;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class LocationService {

	private final LocationRepository locationRepository;

	public LocationService(LocationRepository locationRepository) {
		this.locationRepository = locationRepository;
	}

	public List<LocationResponseDto> getAllLocations() {
		return locationRepository.findAll().stream().map(this::toResponseDto).toList();
	}

	public LocationResponseDto getLocationById(Long id) {
		Location location = locationRepository.findById(id)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Location not found"));
		return toResponseDto(location);
	}

	public LocationResponseDto createLocation(LocationRequestDto requestDto) {
		Location location = new Location();
		location.setName(requestDto.getName());
		location.setAddress(requestDto.getAddress());
		Location savedLocation = locationRepository.save(location);
		return toResponseDto(savedLocation);
	}

	public LocationResponseDto updateLocation(Long id, LocationRequestDto requestDto) {
		Location existingLocation = findLocationByIdOrThrow(id);
		existingLocation.setName(requestDto.getName());
		existingLocation.setAddress(requestDto.getAddress());
		Location savedLocation = locationRepository.save(existingLocation);
		return toResponseDto(savedLocation);
	}

	public void deleteLocation(Long id) {
		Location location = findLocationByIdOrThrow(id);
		locationRepository.delete(location);
	}

	private Location findLocationByIdOrThrow(Long id) {
		return locationRepository.findById(id)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Location not found"));
	}

	private LocationResponseDto toResponseDto(Location location) {
		return LocationResponseDto.builder()
			.id(location.getId())
			.name(location.getName())
			.address(location.getAddress())
			.build();
	}
}
