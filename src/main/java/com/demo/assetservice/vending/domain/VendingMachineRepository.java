package com.demo.assetservice.vending.domain;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface VendingMachineRepository extends JpaRepository<VendingMachine, Long> {

	List<VendingMachine> findByLocationId(Long locationId);
}
