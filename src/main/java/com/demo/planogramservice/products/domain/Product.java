package com.demo.planogramservice.products.domain;

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
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "products")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Product {
	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private Long id;

	@Column(name = "name", nullable = false, length = 120)
	private String name;

	@Column(name = "sku", nullable = false, unique = true, length = 60)
	private String sku;

	@Column(name = "description", length = 500)
	private String description;

	@Column(name = "brand", length = 120)
	private String brand;

	@Column(name = "category", length = 120)
	private String category;

	@Column(name = "active", nullable = false)
	private Boolean active;

	public Product(Long id) {
		this.id = id;
	}
}
