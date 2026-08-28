package com.demo.planogramservice.vending_products.services;

import com.demo.planogramservice.vending_products.domain.VendingProduct;
import com.demo.planogramservice.vending_products.domain.VendingProductRepository;
import com.demo.planogramservice.vending_products.dto.VendingProductRequestDto;
import com.demo.planogramservice.vending_products.dto.VendingProductResponseDto;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class VendingProductService {

	private final VendingProductRepository vendingProductRepository;

	public VendingProductService(VendingProductRepository vendingProductRepository) {
		this.vendingProductRepository = vendingProductRepository;
	}

	public List<VendingProductResponseDto> getAllVendingProducts() {
		return vendingProductRepository.findAll().stream().map(this::toResponseDto).toList();
	}

	public VendingProductResponseDto getVendingProductById(Long id) {
		VendingProduct vendingProduct = findByIdOrThrow(id);
		return toResponseDto(vendingProduct);
	}

	public List<VendingProductResponseDto> getVendingProductsByVendingMachineId(Long vendingMachineId) {
		return vendingProductRepository.findByVendingMachineId(vendingMachineId)
			.stream()
			.map(this::toResponseDto)
			.toList();
	}

	public VendingProductResponseDto createVendingProduct(VendingProductRequestDto requestDto) {
		VendingProduct vendingProduct = VendingProduct.builder()
			.vendingMachineId(requestDto.getVendingMachineId())
			.slotNumber(requestDto.getSlotNumber())
			.capacity(requestDto.getCapacity())
			.quantity(requestDto.getQuantity())
			.price(requestDto.getPrice())
			.active(requestDto.getActive())
			.build();

		return toResponseDto(vendingProductRepository.save(vendingProduct));
	}

	public VendingProductResponseDto updateVendingProduct(Long id, VendingProductRequestDto requestDto) {
		VendingProduct vendingProduct = findByIdOrThrow(id);

		vendingProduct.setVendingMachineId(requestDto.getVendingMachineId());
		vendingProduct.setSlotNumber(requestDto.getSlotNumber());
		vendingProduct.setCapacity(requestDto.getCapacity());
		vendingProduct.setQuantity(requestDto.getQuantity());
		vendingProduct.setPrice(requestDto.getPrice());
		vendingProduct.setActive(requestDto.getActive());

		return toResponseDto(vendingProductRepository.save(vendingProduct));
	}

	public void deleteVendingProduct(Long id) {
		VendingProduct vendingProduct = findByIdOrThrow(id);
		vendingProductRepository.delete(vendingProduct);
	}

	private VendingProduct findByIdOrThrow(Long id) {
		return vendingProductRepository.findById(id)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Vending product not found"));
	}

	private VendingProductResponseDto toResponseDto(VendingProduct vendingProduct) {
		return VendingProductResponseDto.builder()
			.id(vendingProduct.getId())
			.vendingMachineId(vendingProduct.getVendingMachineId())
			.slotNumber(vendingProduct.getSlotNumber())
			.capacity(vendingProduct.getCapacity())
			.quantity(vendingProduct.getQuantity())
			.price(vendingProduct.getPrice())
			.active(vendingProduct.getActive())
			.build();
	}
}
