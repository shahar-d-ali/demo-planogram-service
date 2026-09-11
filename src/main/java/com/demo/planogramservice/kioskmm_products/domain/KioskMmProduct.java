package com.demo.planogramservice.kioskmm_products.domain;

import com.demo.planogramservice.products.domain.Product;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "kioskmm_products")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class KioskMmProduct {

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "kiosk_mm_id", nullable = false)
	private Long kioskMmId;

	@Column(name = "capacity", nullable = false)
	private Integer capacity;

	@Column(name = "quantity", nullable = false)
	private Integer quantity;

	@Column(name = "price", nullable = false)
	private Double price;

	@Column(name = "active", nullable = false)
	private Boolean active;

	@ManyToOne
	@JoinColumn(name = "product_id", nullable = true)
	private Product product;
}
