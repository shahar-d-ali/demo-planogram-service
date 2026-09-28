package com.demo.planogramservice;

import java.util.List;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Contact;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import io.swagger.v3.oas.models.servers.Server;

@SpringBootApplication
public class DemoPlanogramServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(DemoPlanogramServiceApplication.class, args);
	}

	@Bean
	public OpenAPI planogramOpenAPI() {
		return new OpenAPI()
				.info(new Info()
						.title("Demo Planogram Service API")
						.description("Interactive OpenAPI 3.0 & Swagger UI documentation for Planogram Microservice. "
								+ "Covers Product Management, Vending Products, Kiosk Products, and Kafka Messaging / Telemetry.")
						.version("1.0.0")
						.contact(new Contact()
								.name("TRIO Architecture Team")
								.email("dev@demo.com"))
						.license(new License()
								.name("Apache 2.0")
								.url("https://www.apache.org/licenses/LICENSE-2.0")))
				.servers(List.of(
						new Server().url("/api/planogram").description("Service Context Path"),
						new Server().url("http://localhost:8002/api/planogram").description("Direct Local Planogram Service"),
						new Server().url("http://localhost:8000/api/planogram").description("API Gateway Route")
				));
	}
}
