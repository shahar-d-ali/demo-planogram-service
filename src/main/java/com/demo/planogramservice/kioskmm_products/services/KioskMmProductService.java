package com.demo.planogramservice.kioskmm_products.services;

import com.demo.planogramservice.kioskmm_products.domain.KioskMmProduct;
import com.demo.planogramservice.kioskmm_products.domain.KioskMmProductRepository;
import com.demo.planogramservice.kioskmm_products.dto.KioskMmProductRequestDto;
import com.demo.planogramservice.kioskmm_products.dto.KioskMmProductResponseDto;
import java.util.List;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.server.ResponseStatusException;

@Service
public class KioskMmProductService {

	private final KioskMmProductRepository kioskMmProductRepository;

	public KioskMmProductService(KioskMmProductRepository kioskMmProductRepository) {
		this.kioskMmProductRepository = kioskMmProductRepository;
	}

	public List<KioskMmProductResponseDto> getAllKioskMmProducts() {
		return kioskMmProductRepository.findAll().stream().map(this::toResponseDto).toList();
	}

	public KioskMmProductResponseDto getKioskMmProductById(Long id) {
		KioskMmProduct kioskMmProduct = findByIdOrThrow(id);
		return toResponseDto(kioskMmProduct);
	}

	public List<KioskMmProductResponseDto> getKioskMmProductsByKioskMmId(Long kioskMmId) {
		return kioskMmProductRepository.findByKioskMmId(kioskMmId)
			.stream()
			.map(this::toResponseDto)
			.toList();
	}

	public KioskMmProductResponseDto createKioskMmProduct(KioskMmProductRequestDto requestDto) {
		KioskMmProduct kioskMmProduct = KioskMmProduct.builder()
			.kioskMmId(requestDto.getKioskMmId())
			.capacity(requestDto.getCapacity())
			.quantity(requestDto.getQuantity())
			.price(requestDto.getPrice())
			.active(requestDto.getActive())
			.build();

		return toResponseDto(kioskMmProductRepository.save(kioskMmProduct));
	}

	public KioskMmProductResponseDto updateKioskMmProduct(Long id, KioskMmProductRequestDto requestDto) {
		KioskMmProduct kioskMmProduct = findByIdOrThrow(id);

		kioskMmProduct.setKioskMmId(requestDto.getKioskMmId());
		kioskMmProduct.setCapacity(requestDto.getCapacity());
		kioskMmProduct.setQuantity(requestDto.getQuantity());
		kioskMmProduct.setPrice(requestDto.getPrice());
		kioskMmProduct.setActive(requestDto.getActive());

		return toResponseDto(kioskMmProductRepository.save(kioskMmProduct));
	}

	public void deleteKioskMmProduct(Long id) {
		KioskMmProduct kioskMmProduct = findByIdOrThrow(id);
		kioskMmProductRepository.delete(kioskMmProduct);
	}

	private KioskMmProduct findByIdOrThrow(Long id) {
		return kioskMmProductRepository.findById(id)
			.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Kiosk micro-market product not found"));
	}

	private KioskMmProductResponseDto toResponseDto(KioskMmProduct kioskMmProduct) {
		return KioskMmProductResponseDto.builder()
			.id(kioskMmProduct.getId())
			.kioskMmId(kioskMmProduct.getKioskMmId())
			.productId(kioskMmProduct.getProduct() != null ? kioskMmProduct.getProduct().getId() : null)
			.capacity(kioskMmProduct.getCapacity())
			.quantity(kioskMmProduct.getQuantity())
			.price(kioskMmProduct.getPrice())
			.active(kioskMmProduct.getActive())
			.build();
	}
}
