package com.demo.planogramservice.kioskmm_products.domain;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface KioskMmProductRepository extends JpaRepository<KioskMmProduct, Long> {

	List<KioskMmProduct> findByKioskMmId(Long kioskMmId);
}
