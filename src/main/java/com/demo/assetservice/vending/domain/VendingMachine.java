package com.demo.assetservice.vending.domain;

import com.demo.assetservice.location.domain.Location;
import com.fasterxml.jackson.annotation.JsonBackReference;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "vending_machines")
@Data
@AllArgsConstructor
@NoArgsConstructor
public class VendingMachine {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "machine_code", nullable = false, unique = true, length = 50)
	private String machineCode;

	@Column(name = "model", length = 100)
	private String model;

	@Column(name = "status", length = 30)
	private String status;

	@JsonBackReference
	@ManyToOne(fetch = FetchType.LAZY)
	@JoinColumn(name = "location_id", nullable = false)
	private Location location;

}