package io.changelens;

import io.changelens.support.IntegrationTestContainers;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(IntegrationTestContainers.class)
class ChangelensApplicationTests {

	@Test
	void contextLoads() {
	}

}
