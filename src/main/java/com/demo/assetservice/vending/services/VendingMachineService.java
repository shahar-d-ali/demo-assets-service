package com.demo.assetservice.vending.services;

import com.demo.assetservice.location.domain.Location;
import com.demo.assetservice.location.domain.LocationRepository;
import com.demo.assetservice.vending.domain.VendingMachine;
import com.demo.assetservice.vending.domain.VendingMachineRepository;
import com.demo.assetservice.vending.dto.VendingMachineRequestDto;
import com.demo.assetservice.vending.dto.VendingMachineResponseDto;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class VendingMachineService {

	private final VendingMachineRepository vendingMachineRepository;
	private final LocationRepository locationRepository;

	public VendingMachineService(
		VendingMachineRepository vendingMachineRepository,
		LocationRepository locationRepository
	) {
		this.vendingMachineRepository = vendingMachineRepository;
		this.locationRepository = locationRepository;
	}

	public List<VendingMachineResponseDto> getAllVendingMachines() {
		return vendingMachineRepository.findAll().stream().map(this::toResponseDto).toList();
	}

	public VendingMachineResponseDto getVendingMachineById(Long id) {
		VendingMachine vendingMachine = vendingMachineRepository.findById(id)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Vending machine not found"));
		return toResponseDto(vendingMachine);
	}

	public List<VendingMachineResponseDto> getVendingMachinesByLocationId(Long locationId) {
		return vendingMachineRepository.findByLocationId(locationId).stream().map(this::toResponseDto).toList();
	}

	public VendingMachineResponseDto createVendingMachine(VendingMachineRequestDto requestDto, Long locationId) {
		Location location = locationRepository.findById(locationId)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Location not found"));

		VendingMachine vendingMachine = new VendingMachine();
		vendingMachine.setMachineCode(requestDto.getMachineCode());
		vendingMachine.setModel(requestDto.getModel());
		vendingMachine.setStatus(requestDto.getStatus());
		vendingMachine.setLocation(location);
		VendingMachine savedVendingMachine = vendingMachineRepository.save(vendingMachine);
		return toResponseDto(savedVendingMachine);
	}

	public VendingMachineResponseDto updateVendingMachine(Long id, VendingMachineRequestDto requestDto, Long locationId) {
		VendingMachine existingVendingMachine = findVendingMachineByIdOrThrow(id);

		existingVendingMachine.setMachineCode(requestDto.getMachineCode());
		existingVendingMachine.setModel(requestDto.getModel());
		existingVendingMachine.setStatus(requestDto.getStatus());

		if (locationId != null) {
			Location location = locationRepository.findById(locationId)
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Location not found"));
			existingVendingMachine.setLocation(location);
		}

		VendingMachine savedVendingMachine = vendingMachineRepository.save(existingVendingMachine);
		return toResponseDto(savedVendingMachine);
	}

	public void deleteVendingMachine(Long id) {
		VendingMachine vendingMachine = findVendingMachineByIdOrThrow(id);
		vendingMachineRepository.delete(vendingMachine);
	}

	private VendingMachine findVendingMachineByIdOrThrow(Long id) {
		return vendingMachineRepository.findById(id)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Vending machine not found"));
	}

	private VendingMachineResponseDto toResponseDto(VendingMachine vendingMachine) {
		Long locationId = vendingMachine.getLocation() != null ? vendingMachine.getLocation().getId() : null;

		return VendingMachineResponseDto.builder()
			.id(vendingMachine.getId())
			.machineCode(vendingMachine.getMachineCode())
			.locationName(vendingMachine.getLocation().getName())
			.model(vendingMachine.getModel())
			.status(vendingMachine.getStatus())
			.locationId(locationId)
			.build();
	}
}
