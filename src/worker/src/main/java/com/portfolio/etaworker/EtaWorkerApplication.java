package com.portfolio.etaworker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class EtaWorkerApplication {

	public static void main(String[] args) {
		SpringApplication.run(EtaWorkerApplication.class, args);
	}

}
