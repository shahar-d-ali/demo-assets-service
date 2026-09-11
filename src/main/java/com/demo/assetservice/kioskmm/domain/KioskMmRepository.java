package com.demo.assetservice.kioskmm.domain;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface KioskMmRepository extends JpaRepository<KioskMm, Long> {

	List<KioskMm> findByLocationId(Long locationId);
}
