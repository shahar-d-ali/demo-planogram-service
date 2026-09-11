package com.demo.planogramservice.kioskmm_products.api;

import com.demo.planogramservice.kioskmm_products.dto.KioskMmProductRequestDto;
import com.demo.planogramservice.kioskmm_products.dto.KioskMmProductResponseDto;
import com.demo.planogramservice.kioskmm_products.services.KioskMmProductService;
import java.util.List;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/kiosk-mm-products")
public class KioskMmProductController {

	private final KioskMmProductService kioskMmProductService;

	public KioskMmProductController(KioskMmProductService kioskMmProductService) {
		this.kioskMmProductService = kioskMmProductService;
	}

	@GetMapping
	public List<KioskMmProductResponseDto> getKioskMmProducts(
		@RequestParam(required = false) Long kioskMmId
	) {
		if (kioskMmId != null) {
			return kioskMmProductService.getKioskMmProductsByKioskMmId(kioskMmId);
		}
		return kioskMmProductService.getAllKioskMmProducts();
	}

	@GetMapping("/{id}")
	public KioskMmProductResponseDto getKioskMmProductById(@PathVariable Long id) {
		return kioskMmProductService.getKioskMmProductById(id);
	}

	@PostMapping
	public KioskMmProductResponseDto createKioskMmProduct(@RequestBody KioskMmProductRequestDto requestDto) {
		return kioskMmProductService.createKioskMmProduct(requestDto);
	}

	@PutMapping("/{id}")
	public KioskMmProductResponseDto updateKioskMmProduct(
		@PathVariable Long id,
		@RequestBody KioskMmProductRequestDto requestDto
	) {
		return kioskMmProductService.updateKioskMmProduct(id, requestDto);
	}

	@DeleteMapping("/{id}")
	public void deleteKioskMmProduct(@PathVariable Long id) {
		kioskMmProductService.deleteKioskMmProduct(id);
	}
}
