package com.demo.planogramservice.products.services;

import com.demo.planogramservice.products.domain.Product;
import com.demo.planogramservice.products.domain.ProductRepository;
import com.demo.planogramservice.products.dto.ProductRequestDto;
import com.demo.planogramservice.products.dto.ProductResponseDto;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class ProductService {
	private final ProductRepository productRepository;

	public ProductService(ProductRepository productRepository) {
		this.productRepository = productRepository;
	}

	public List<ProductResponseDto> getAllProducts() {
		return productRepository.findAll().stream().map(this::toResponseDto).toList();
	}

	public ProductResponseDto getProductById(Long id) {
		Product product = findByIdOrThrow(id);
		return toResponseDto(product);
	}

	public ProductResponseDto createProduct(ProductRequestDto requestDto) {

		Product product = Product.builder()
			.name(requestDto.getName())
			.sku(requestDto.getSku())
			.description(requestDto.getDescription())
			.brand(requestDto.getBrand())
			.category(requestDto.getCategory())
			.active(requestDto.getActive())
			.build();

		return toResponseDto(productRepository.save(product));
	}

	public ProductResponseDto updateProduct(Long id, ProductRequestDto requestDto) {
		Product product = findByIdOrThrow(id);
		product.setName(requestDto.getName());
		product.setSku(requestDto.getSku());
		product.setDescription(requestDto.getDescription());
		product.setBrand(requestDto.getBrand());
		product.setCategory(requestDto.getCategory());
		product.setActive(requestDto.getActive());

		return toResponseDto(productRepository.save(product));
	}

	public void deleteProduct(Long id) {
		Product product = findByIdOrThrow(id);
		productRepository.delete(product);
	}

	private Product findByIdOrThrow(Long id) {
		return productRepository.findById(id)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Product not found"));
	}


	private ProductResponseDto toResponseDto(Product product) {

		return ProductResponseDto.builder()
			.id(product.getId())
			.name(product.getName())
			.sku(product.getSku())
			.description(product.getDescription())
			.brand(product.getBrand())
			.category(product.getCategory())
			.active(product.getActive())
			.build();
	}
}
