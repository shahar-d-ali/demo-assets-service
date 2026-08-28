package com.demo.assetservice.vending.api;

import com.demo.assetservice.vending.dto.VendingMachineRequestDto;
import com.demo.assetservice.vending.dto.VendingMachineResponseDto;
import com.demo.assetservice.vending.services.VendingMachineService;
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
@RequestMapping("/vending-machines")
public class VendingMachineController {

	private final VendingMachineService vendingMachineService;

	public VendingMachineController(VendingMachineService vendingMachineService) {
		this.vendingMachineService = vendingMachineService;
	}

	@GetMapping
	public List<VendingMachineResponseDto> getAllVendingMachines(
		@RequestParam(required = false) Long locationId
	) {
		if (locationId != null) {
			return vendingMachineService.getVendingMachinesByLocationId(locationId);
		}
		return vendingMachineService.getAllVendingMachines();
	}

	@GetMapping("/{id}")
	public VendingMachineResponseDto getVendingMachineById(@PathVariable Long id) {
		return vendingMachineService.getVendingMachineById(id);
	}

	@PostMapping
	public VendingMachineResponseDto createVendingMachine(
		@RequestBody VendingMachineRequestDto requestDto,
		@RequestParam Long locationId
	) {
		return vendingMachineService.createVendingMachine(requestDto, locationId);
	}

	@PutMapping("/{id}")
	public VendingMachineResponseDto updateVendingMachine(
		@PathVariable Long id,
		@RequestBody VendingMachineRequestDto requestDto,
		@RequestParam(required = false) Long locationId
	) {
		return vendingMachineService.updateVendingMachine(id, requestDto, locationId);
	}

	@DeleteMapping("/{id}")
	public void deleteVendingMachine(@PathVariable Long id) {
		vendingMachineService.deleteVendingMachine(id);
	}
}
