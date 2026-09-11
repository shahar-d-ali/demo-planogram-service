/* Response payload for kiosk micro-market product endpoints. */
package com.demo.planogramservice.kioskmm_products.dto;

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
public class KioskMmProductResponseDto {

	private Long id;
	private Long kioskMmId;
	private Long productId;
	private Integer capacity;
	private Integer quantity;
	private Double price;
	private Boolean active;
}
