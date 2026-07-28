package com.iip.infra;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The proof test called out in Original Specification §9 and required by
 * the Definition of Done for Release 1's exit criterion: one HTTP
 * submission through the real Source Service, and both real targets
 * (Postgres, the CSV file) end up with the record.
 *
 * This lives in infra, not any single service's repo, because it's the
 * only place that's ever known how to wire all three services together
 * (docker-compose.yml already builds all three images from these exact
 * relative paths) -- a test spanning three separate Maven projects can't
 * live inside any one of them without breaking independent
 * deployability. It builds the *real* Docker images (not `mvn
 * spring-boot:run`) from each service's own Dockerfile, which is a
 * stronger proof than an in-JVM multi-context trick would be: it
 * exercises the actual deployment artifacts, not just the source.
 */
class FullPipelineEndToEndTest {

	private static final Network network = Network.newNetwork();

	private static GenericContainer<?> kafka;
	private static PostgreSQLContainer postgres;
	private static GenericContainer<?> contractRegistry;
	private static GenericContainer<?> sourceService;
	private static GenericContainer<?> dbAdapter;
	private static GenericContainer<?> fileAdapter;

	@BeforeAll
	static void startThePipeline() throws Exception {
		// A plain GenericContainer, not Testcontainers' KafkaContainer
		// wrapper -- that class only advertises the host-mapped address,
		// which is meaningless for container-to-container traffic (the
		// classic Kafka advertised-listener trap, see infra/README.md).
		// This test never needs host access to Kafka at all -- only the
		// three app containers talk to it, all on this network -- so a
		// single internal listener advertised as "kafka:9092" is enough,
		// mirroring docker-compose.yml's own proven-working PLAINTEXT
		// listener minus the host-facing one this test doesn't need.
		kafka = new GenericContainer<>(DockerImageName.parse("apache/kafka:4.3.1"))
				.withNetwork(network)
				.withNetworkAliases("kafka")
				.withExposedPorts(9092)
				.withEnv("KAFKA_NODE_ID", "1")
				.withEnv("KAFKA_PROCESS_ROLES", "broker,controller")
				.withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
				.withEnv("KAFKA_LISTENERS", "CONTROLLER://:29093,PLAINTEXT://:9092")
				.withEnv("KAFKA_ADVERTISED_LISTENERS", "PLAINTEXT://kafka:9092")
				.withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@kafka:29093")
				.withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "PLAINTEXT")
				.withEnv("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
				.withEnv("CLUSTER_ID", "4L6g3nShT-eMCtK--X86sw")
				.withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
				.withEnv("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
				.withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
				.withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
				.withEnv("KAFKA_LOG_DIRS", "/tmp/kraft-combined-logs");
		kafka.start();

		postgres = new PostgreSQLContainer(DockerImageName.parse("postgres:17-alpine"))
				.withNetwork(network)
				.withNetworkAliases("postgres")
				.withDatabaseName("iip")
				.withUsername("iip")
				.withPassword("iip")
				// The whole directory, in lexical order, exactly as compose
				// mounts it -- the registry's tables come from 02-registry.sql
				// and the contract-registry container will not start without
				// them (ddl-auto: validate).
				.withCopyFileToContainer(
						MountableFile.forHostPath(Path.of("../postgres/01-interns.sql")),
						"/docker-entrypoint-initdb.d/01-interns.sql")
				.withCopyFileToContainer(
						MountableFile.forHostPath(Path.of("../postgres/02-registry.sql")),
						"/docker-entrypoint-initdb.d/02-registry.sql");
		postgres.start();

		// Release 4: the source-service has no compiled-in schema *and* no
		// contract file. Its contracts come from here, which makes the
		// registry a member of the pipeline this test claims to prove rather
		// than a detail of it -- if the registry is empty or unreachable, the
		// pipeline genuinely does not work, and the test should say so.
		contractRegistry = new GenericContainer<>(
				new ImageFromDockerfile("iip/contract-registry-e2e", false)
						.withFileFromPath(".", Path.of("../../contract-registry")))
				.withNetwork(network)
				.withNetworkAliases("contract-registry")
				.withExposedPorts(8083)
				.withEnv("DB_URL", "jdbc:postgresql://postgres:5432/iip")
				.withEnv("DB_USERNAME", "iip")
				.withEnv("DB_PASSWORD", "iip")
				.withEnv("SERVER_PORT", "8083")
				.waitingFor(Wait.forHttp("/actuator/health").forPort(8083).withStartupTimeout(Duration.ofMinutes(3)));
		contractRegistry.start();

		registerTheDeploymentsContracts();

		sourceService = new GenericContainer<>(
				new ImageFromDockerfile("iip/source-service-e2e", false)
						.withFileFromPath(".", Path.of("../../source-service")))
				.withNetwork(network)
				.withNetworkAliases("source-service")
				.withExposedPorts(8080)
				.withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
				.withEnv("SERVER_PORT", "8080")
				.withEnv("CONTRACTS_SOURCE", "registry")
				.withEnv("CONTRACT_REGISTRY_URL", "http://contract-registry:8083")
				.waitingFor(Wait.forHttp("/actuator/health").forPort(8080).withStartupTimeout(Duration.ofMinutes(3)));
		sourceService.start();

		dbAdapter = new GenericContainer<>(
				new ImageFromDockerfile("iip/db-adapter-e2e", false)
						.withFileFromPath(".", Path.of("../../db-adapter")))
				.withNetwork(network)
				.withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
				.withEnv("DB_URL", "jdbc:postgresql://postgres:5432/iip")
				.withEnv("DB_USERNAME", "iip")
				.withEnv("DB_PASSWORD", "iip")
				.withEnv("SERVER_PORT", "8081")
				.withExposedPorts(8081)
				.waitingFor(Wait.forHttp("/actuator/health").forPort(8081).withStartupTimeout(Duration.ofMinutes(3)));
		dbAdapter.start();

		fileAdapter = new GenericContainer<>(
				new ImageFromDockerfile("iip/file-adapter-e2e", false)
						.withFileFromPath(".", Path.of("../../file-adapter")))
				.withNetwork(network)
				.withEnv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
				.withEnv("SERVER_PORT", "8082")
				.withEnv("FILE_OUTPUT_PATH", "/data/interns.csv")
				.withEnv("DEDUP_STORE_PATH", "/data/file-adapter-dedup")
				.withExposedPorts(8082)
				.waitingFor(Wait.forHttp("/actuator/health").forPort(8082).withStartupTimeout(Duration.ofMinutes(3)));
		fileAdapter.start();
	}

	/**
	 * Seeds the registry through the same public {@code POST /contracts} the
	 * compose stack's {@code contract-registry-init} job uses, from the same
	 * {@code infra/contracts/*.json} files. Not a test fixture: it is the
	 * deployment's starting state, and reading it from disk here is what stops
	 * this test from proving a pipeline whose contracts differ from the one an
	 * operator actually gets.
	 */
	private static void registerTheDeploymentsContracts() throws Exception {
		HttpClient http = HttpClient.newHttpClient();
		String registryUrl = "http://" + contractRegistry.getHost() + ":" + contractRegistry.getMappedPort(8083);

		try (var files = java.nio.file.Files.list(Path.of("../contracts"))) {
			for (Path contractFile : files.filter(f -> f.toString().endsWith(".json")).toList()) {
				HttpResponse<String> response = http.send(
						HttpRequest.newBuilder(URI.create(registryUrl + "/contracts"))
								.header("Content-Type", "application/json")
								.POST(HttpRequest.BodyPublishers.ofFile(contractFile))
								.build(),
						HttpResponse.BodyHandlers.ofString());

				assertTrue(response.statusCode() == 200 || response.statusCode() == 201,
						"registering " + contractFile.getFileName() + " failed with "
								+ response.statusCode() + ": " + response.body());
			}
		}
	}

	@AfterAll
	static void stopThePipeline() {
		if (fileAdapter != null) fileAdapter.stop();
		if (dbAdapter != null) dbAdapter.stop();
		if (sourceService != null) sourceService.stop();
		if (contractRegistry != null) contractRegistry.stop();
		if (postgres != null) postgres.stop();
		if (kafka != null) kafka.stop();
		network.close();
	}

	@Test
	void submittingAnInternViaHttpLandsInPostgresAndCsv() throws Exception {
		String internId = "INT-E2E-PROOF-" + UUID.randomUUID();
		String requestBody = """
				{
				  "internId": "%s",
				  "firstName": "Ada",
				  "lastName": "Lovelace",
				  "email": "ada@example.com",
				  "college": "MIT",
				  "department": "Platform Engineering",
				  "mentor": "Sam",
				  "startDate": "2026-09-01"
				}
				""".formatted(internId);

		HttpClient client = HttpClient.newHttpClient();
		HttpRequest request = HttpRequest.newBuilder()
				.uri(URI.create("http://" + sourceService.getHost() + ":" + sourceService.getMappedPort(8080) + "/interns"))
				.header("Content-Type", "application/json")
				.POST(HttpRequest.BodyPublishers.ofString(requestBody))
				.build();

		HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
		assertEquals(202, response.statusCode(), "expected the real HTTP submission to be accepted: " + response.body());

		String jdbcUrl = "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432) + "/iip";
		boolean foundInPostgres = false;
		for (int i = 0; i < 30 && !foundInPostgres; i++) {
			Thread.sleep(1000);
			try (Connection conn = DriverManager.getConnection(jdbcUrl, "iip", "iip");
					PreparedStatement stmt = conn.prepareStatement("SELECT first_name FROM interns WHERE intern_id = ?")) {
				stmt.setString(1, internId);
				try (ResultSet rs = stmt.executeQuery()) {
					if (rs.next()) {
						foundInPostgres = true;
						assertEquals("Ada", rs.getString("first_name"));
					}
				}
			}
		}
		if (!foundInPostgres) {
			// Containers are torn down in @AfterAll, so without this a
			// failure here is nearly undebuggable afterward.
			System.out.println("===== db-adapter logs =====");
			System.out.println(dbAdapter.getLogs());
		}
		assertTrue(foundInPostgres, "expected a Postgres row for " + internId + " via the Database Adapter");

		boolean foundInCsv = false;
		for (int i = 0; i < 30 && !foundInCsv; i++) {
			Thread.sleep(1000);
			Container.ExecResult result = fileAdapter.execInContainer("cat", "/data/interns.csv");
			if (result.getStdout().contains(internId)) {
				foundInCsv = true;
			}
		}
		if (!foundInCsv) {
			System.out.println("===== file-adapter logs =====");
			System.out.println(fileAdapter.getLogs());
		}
		assertTrue(foundInCsv, "expected a CSV line for " + internId + " via the File Adapter");
	}
}
