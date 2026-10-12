package com.demo.planogramservice.vending_products.api;

import com.demo.planogramservice.vending_products.dto.VendingProductRequestDto;
import com.demo.planogramservice.vending_products.dto.VendingProductResponseDto;
import com.demo.planogramservice.vending_products.services.VendingProductService;
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

import io.swagger.v3.oas.annotations.tags.Tag;

@Tag(name = "Vending Products", description = "Vending Machine Planogram Product Slot APIs")
@RestController
@RequestMapping("/vending-products")
public class VendingProductController {

	private final VendingProductService vendingProductService;

	public VendingProductController(VendingProductService vendingProductService) {
		this.vendingProductService = vendingProductService;
	}

	@GetMapping
	public List<VendingProductResponseDto> getVendingProducts(
		@RequestParam(required = false) Long vendingMachineId
	) {
		if (vendingMachineId != null) {
			return vendingProductService.getVendingProductsByVendingMachineId(vendingMachineId);
		}
		return vendingProductService.getAllVendingProducts();
	}

	@GetMapping("/{id}")
	public VendingProductResponseDto getVendingProductById(@PathVariable Long id) {
		return vendingProductService.getVendingProductById(id);
	}

	@PostMapping
	public VendingProductResponseDto createVendingProduct(@RequestBody VendingProductRequestDto requestDto) {
		return vendingProductService.createVendingProduct(requestDto);
	}

	@PutMapping("/{id}")
	public VendingProductResponseDto updateVendingProduct(
		@PathVariable Long id,
		@RequestBody VendingProductRequestDto requestDto
	) {
		return vendingProductService.updateVendingProduct(id, requestDto);
	}

	@DeleteMapping("/{id}")
	public void deleteVendingProduct(@PathVariable Long id) {
		vendingProductService.deleteVendingProduct(id);
	}
}
