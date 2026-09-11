package com.demo.assetservice.kioskmm.dto;

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
public class KioskMmRequestDto {

	private String kioskCode;
	private String model;
	private String status;
}
