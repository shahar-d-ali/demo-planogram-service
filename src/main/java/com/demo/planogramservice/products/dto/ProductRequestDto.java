package com.demo.planogramservice.products.dto;

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
public class ProductRequestDto {
	private String name;
	private String sku;
	private String description;
	private String brand;
	private String category;
	private Boolean active;
}
