package com.demo.assetservice.vending.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class VendingMachineRequestDto {

	private String machineCode;
	private String model;
	private String status;
}