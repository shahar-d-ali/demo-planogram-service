package com.demo.planogramservice.vending_products.domain;

import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;

public interface VendingProductRepository extends JpaRepository<VendingProduct, Long> {

	List<VendingProduct> findByVendingMachineId(Long vendingMachineId);
}
