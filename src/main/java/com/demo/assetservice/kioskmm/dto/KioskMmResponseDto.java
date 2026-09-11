/* Response payload for kiosk micro-market endpoints. */
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
public class KioskMmResponseDto {

	private Long id;
	private String kioskCode;
	private String model;
	private String status;
	private Long locationId;
	private String locationName;
}
