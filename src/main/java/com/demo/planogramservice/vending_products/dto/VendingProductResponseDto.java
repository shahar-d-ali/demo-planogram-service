package com.demo.planogramservice.vending_products.dto;

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
public class VendingProductResponseDto {

	private Long id;
	private Long vendingMachineId;
	private Long productId;
	private String slotNumber;
	private Integer capacity;
	private Integer quantity;
	private Double price;
	private Boolean active;
}
