package com.iip.infra;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.containers.BindMode;
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
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class FullPipelineEndToEndTest {

	private static final Network network = Network.newNetwork();

	private static GenericContainer<?> kafka;
	private static PostgreSQLContainer postgres;
	private static GenericContainer<?> schemaRegistry;
	private static GenericContainer<?> contractRegistry;
	private static GenericContainer<?> sourceService;
	private static GenericContainer<?> dbAdapter;
	private static GenericContainer<?> fileAdapter;

	/**
	 * A shell on the pipeline's own network, with the repository bind-mounted
	 * read-only at {@code /workspace}. It exists so infra's scripts can be run
	 * as scripts -- the same {@code register-envelope.sh} the compose stack
	 * runs, and the same {@code compatibility-gate.sh} CI runs -- rather than
	 * reimplemented in Java here. A test that reimplements the script proves
	 * the reimplementation works.
	 */
	private static GenericContainer<?> toolbox;

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
				//
				// Copied as a directory rather than file-by-file on purpose.
				// The enumerated version said "the whole directory" in this
				// comment while actually naming two files, so Phase 5.1's
				// 03-records.sql would have been silently absent here and the
				// generic write path would have failed against a table the
				// repository plainly contains.
				.withCopyFileToContainer(
						MountableFile.forHostPath(Path.of("../postgres")),
						"/docker-entrypoint-initdb.d/");
		postgres.start();

		// Phase 4.6/4.7. Not scenery: after Phase 4.8 none of the three
		// service images carries the envelope schema either, so all three
		// fetch it from here at startup and none of them will start without
		// it. If this container or the registration below is broken, the
		// pipeline genuinely does not exist, and the test says so.
		schemaRegistry = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.7.1"))
				.withNetwork(network)
				.withNetworkAliases("schema-registry")
				.withExposedPorts(8081)
				.withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
				.withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
				.withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
				.withEnv("SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL", "BACKWARD")
				.waitingFor(Wait.forHttp("/subjects").forPort(8081).withStartupTimeout(Duration.ofMinutes(3)));
		schemaRegistry.start();

		toolbox = new GenericContainer<>(DockerImageName.parse("alpine:3.20"))
				.withNetwork(network)
				.withFileSystemBind(Path.of("../..").toAbsolutePath().normalize().toString(),
						"/workspace", BindMode.READ_ONLY)
				.withCommand("sh", "-c",
						"apk add --no-cache curl jq >/dev/null && echo TOOLBOX_READY && sleep infinity")
				.waitingFor(Wait.forLogMessage(".*TOOLBOX_READY.*", 1).withStartupTimeout(Duration.ofMinutes(3)));
		toolbox.start();

		registerTheEnvelopeSchema();

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
				// 2s against 30s in production. Phase 4.11's claim is that a
				// contract change goes live with nothing redeployed; the
				// interval is how long that takes, not whether it happens.
				.withEnv("CONTRACT_REFRESH_INTERVAL_MS", "2000")
				.withEnv("ENVELOPE_SCHEMA_SOURCE", "registry")
				.withEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
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
				.withEnv("ENVELOPE_SCHEMA_SOURCE", "registry")
				.withEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
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
				.withEnv("ENVELOPE_SCHEMA_SOURCE", "registry")
				.withEnv("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
				.withExposedPorts(8082)
				.waitingFor(Wait.forHttp("/actuator/health").forPort(8082).withStartupTimeout(Duration.ofMinutes(3)));
		fileAdapter.start();
	}

	/**
	 * Registers the envelope schema by running infra's own
	 * {@code register-envelope.sh} -- the same script {@code
	 * schema-registry-init} runs on every {@code docker compose up}. Running
	 * the script rather than reimplementing its three HTTP calls is the point:
	 * a bug in the script is a bug in every deployment, and would be invisible
	 * to a Java reimplementation of what the script was supposed to do.
	 */
	private static void registerTheEnvelopeSchema() throws Exception {
		Container.ExecResult result = toolbox.execInContainer("sh", "-c",
				"SCHEMA_REGISTRY_URL=http://schema-registry:8081 sh /workspace/infra/schemas/register-envelope.sh");

		assertEquals(0, result.getExitCode(),
				"registering the envelope schema failed:\n" + result.getStdout() + result.getStderr());
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
		if (schemaRegistry != null) schemaRegistry.stop();
		if (toolbox != null) toolbox.stop();
		if (postgres != null) postgres.stop();
		if (kafka != null) kafka.stop();
		network.close();
	}

	@Test
	@Order(1)
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

		boolean foundInPostgres = awaitPostgresRow(internId, "Ada");
		if (!foundInPostgres) {
			// Containers are torn down in @AfterAll, so without this a
			// failure here is nearly undebuggable afterward.
			System.out.println("===== db-adapter logs =====");
			System.out.println(dbAdapter.getLogs());
		}
		assertTrue(foundInPostgres, "expected a Postgres row for " + internId + " via the Database Adapter");

		boolean foundInCsv = awaitCsvLine(internId);
		if (!foundInCsv) {
			System.out.println("===== file-adapter logs =====");
			System.out.println(fileAdapter.getLogs());
		}
		assertTrue(foundInCsv, "expected a CSV line for " + internId + " via the File Adapter");
	}

	// --- Phase 4.10: the compatibility gate ---------------------------------

	/**
	 * The gate, against the real registries, over the real files. This is the
	 * pass case, and it is worth asserting on its own: a gate that fails on a
	 * clean tree gets disabled within a week, and then it is not a gate.
	 */
	@Test
	@Order(2)
	void theCompatibilityGatePassesOnTheRepositoryAsItStands() throws Exception {
		Container.ExecResult result = runGate("/workspace/infra", "/workspace");

		assertEquals(0, result.getExitCode(),
				"the gate should pass on an unmodified tree:\n" + result.getStdout() + result.getStderr());
		assertTrue(result.getStdout().contains("compatibility gate passed"), result.getStdout());
	}

	/**
	 * <strong>Phase 4.10's done-when: a deliberately-breaking change proves
	 * the gate actually blocks a merge.</strong>
	 *
	 * <p>The change is a removal of a required field from the interns
	 * contract -- a perfectly valid contract document, which is exactly why
	 * only a compatibility check can catch it. The file is edited in a copy of
	 * the tree, so what fails is the gate rather than the repository.
	 */
	@Test
	@Order(3)
	void theGateBlocksAContractChangeThatWouldBreakDeployedConsumers() throws Exception {
		String tree = prepareTreeCopy("breaking-contract");
		// jq rewrites the file rather than a regex: the point is to produce a
		// contract that is still valid JSON and still a usable contract, so
		// that only the compatibility rule can object to it.
		Container.ExecResult edit = exec(
				"jq 'del(.fields[] | select(.name == \"email\"))' " + tree + "/infra/contracts/interns.json > /tmp/i.json"
						+ " && mv /tmp/i.json " + tree + "/infra/contracts/interns.json");
		// Asserted, so a failed edit cannot masquerade as a gate that let a
		// breaking change through.
		assertEquals(0, edit.getExitCode(), edit.getStderr());

		Container.ExecResult result = runGate(tree + "/infra", tree);

		assertEquals(1, result.getExitCode(), "the gate must fail:\n" + result.getStdout() + result.getStderr());
		String output = result.getStdout() + result.getStderr();
		assertTrue(output.contains("interns"), output);
		assertTrue(output.contains("email"), "the gate should say what would break: " + output);
		assertTrue(output.contains("compatibility gate FAILED"), output);
	}

	/**
	 * The other half of the gate, and the failure it exists to catch that
	 * nothing else would: each service keeps its own copy of envelope.json
	 * under src/test/resources so its tests can run without a registry. A
	 * drifted copy means a suite that passes against a schema production does
	 * not use, and every one of those suites would stay green.
	 */
	@Test
	@Order(4)
	void theGateBlocksAServiceTestFixtureThatHasDriftedFromTheRealSchema() throws Exception {
		String tree = prepareTreeCopy("drifted-fixture");
		Container.ExecResult edit = exec("jq '.properties.recordId.description = \"drifted\"' "
				+ tree + "/db-adapter/src/test/resources/schemas/envelope.json > /tmp/e.json"
				+ " && mv /tmp/e.json " + tree + "/db-adapter/src/test/resources/schemas/envelope.json");
		assertEquals(0, edit.getExitCode(), edit.getStderr());

		Container.ExecResult result = runGate(tree + "/infra", tree);

		assertEquals(1, result.getExitCode(), result.getStdout() + result.getStderr());
		assertTrue((result.getStdout() + result.getStderr()).contains("drifted from"),
				result.getStdout() + result.getStderr());
	}

	private Container.ExecResult runGate(String infraDir, String reposDir) throws Exception {
		return toolbox.execInContainer("sh", "-c",
				"SCHEMA_REGISTRY_URL=http://schema-registry:8081"
						+ " CONTRACT_REGISTRY_URL=http://contract-registry:8083"
						+ " IIP_INFRA_DIR=" + infraDir
						+ " IIP_REPOS_DIR=" + reposDir
						+ " sh /workspace/infra/scripts/compatibility-gate.sh");
	}

	/**
	 * A writable copy of just the parts of the repository the gate reads.
	 * {@code /workspace} is mounted read-only on purpose -- a test that could
	 * edit the working tree to prove a point is a test that can corrupt it.
	 */
	private String prepareTreeCopy(String name) throws Exception {
		String tree = "/tmp/" + name;
		Container.ExecResult result = exec(
				"rm -rf " + tree + " && mkdir -p " + tree + "/infra"
						+ " && cp -r /workspace/infra/schemas /workspace/infra/contracts /workspace/infra/scripts "
						+ tree + "/infra/"
						+ " && for svc in source-service db-adapter file-adapter; do"
						+ "   mkdir -p " + tree + "/$svc/src/test/resources/schemas;"
						+ "   cp /workspace/$svc/src/test/resources/schemas/envelope.json "
						+ tree + "/$svc/src/test/resources/schemas/;"
						+ " done");

		assertEquals(0, result.getExitCode(), "could not prepare the tree copy: " + result.getStderr());
		return tree;
	}

	// --- Phase 4.11: evolve a contract, redeploy nothing ---------------------

	/**
	 * <strong>Phase 4.11's done-when, and Release 4's whole argument in one
	 * test: add an optional field to the {@code interns} contract through the
	 * API, redeploy nothing, and confirm the un-restarted adapters still
	 * process new messages correctly.</strong>
	 *
	 * <p>Nothing is restarted here and nothing is rebuilt. The source service
	 * picks the new definition up on its next scheduled refresh; both adapters
	 * are never told at all, and do not need to be -- they read the envelope,
	 * and the envelope did not change. That asymmetry is the payoff for the
	 * split in Data Model 1a: a payload change reaches exactly the one service
	 * that validates payloads.
	 *
	 * <p>Runs last by {@link Order} on purpose. It moves the registry's
	 * {@code interns} contract to schemaVersion 2, after which
	 * infra/contracts/interns.json describes an older definition -- so the
	 * gate test above would then see the repository file as a field removal.
	 * That is the gate being right, not a bug, but it means these two cannot
	 * run in the other order.
	 */
	@Test
	@Order(5)
	void anOptionalFieldAddedThroughTheApiGoesLiveWithNothingRedeployed() throws Exception {
		String registryUrl = "http://" + contractRegistry.getHost() + ":" + contractRegistry.getMappedPort(8083);
		HttpClient client = HttpClient.newHttpClient();

		String before = send(client, HttpRequest.newBuilder(URI.create(registryUrl + "/contracts/interns")).GET());
		assertTrue(before.contains("\"schemaVersion\":1"), before);

		// The evolved definition: interns.json with one optional field added.
		// Built by jq from the file the deployment actually uses, so this
		// cannot accidentally test a contract nobody deploys.
		Container.ExecResult evolved = exec(
				"jq '.fields += [{\"name\":\"githubHandle\",\"type\":\"string\",\"required\":false}]'"
						+ " /workspace/infra/contracts/interns.json"
						+ " | curl -sS -w '\\n%{http_code}' -X POST http://contract-registry:8083/contracts"
						+ " -H 'Content-Type: application/json' --data-binary @-");

		assertEquals(0, evolved.getExitCode(), evolved.getStderr());
		assertTrue(evolved.getStdout().trim().endsWith("200"),
				"evolving the contract should be accepted: " + evolved.getStdout());
		assertTrue(evolved.getStdout().contains("\"schemaVersion\":2"),
				"an accepted change bumps the version: " + evolved.getStdout());

		// Nothing is restarted between here and the submission below.
		String internId = "INT-E2E-EVOLVED-" + UUID.randomUUID();
		String accepted = null;
		for (int attempt = 0; attempt < 30 && accepted == null; attempt++) {
			Thread.sleep(1000);
			HttpResponse<String> response = client.send(
					HttpRequest.newBuilder(URI.create(sourceServiceUrl() + "/contracts/interns/records"))
							.header("Content-Type", "application/json")
							.POST(HttpRequest.BodyPublishers.ofString("""
									{
									  "internId": "%s",
									  "firstName": "Grace",
									  "lastName": "Hopper",
									  "email": "grace@example.com",
									  "college": "Yale",
									  "department": "Platform Engineering",
									  "startDate": "2026-09-01",
									  "githubHandle": "gracehopper"
									}
									""".formatted(internId)))
							.build(),
					HttpResponse.BodyHandlers.ofString());

			// Until the refresh lands, the running service is still validating
			// against v1 and rejects the unknown field -- which is itself the
			// proof that it was validating against the old definition rather
			// than ignoring the field all along.
			if (response.statusCode() == 202) {
				accepted = response.body();
			}
		}

		assertTrue(accepted != null,
				"the new field should have gone live within a few refresh intervals, with nothing redeployed");
		assertTrue(accepted.contains("\"schemaVersion\":2"),
				"the envelope should be stamped with the version it was validated against: " + accepted);

		// And the adapters -- neither restarted, neither aware a contract
		// changed -- still process it, because the envelope did not change.
		assertTrue(awaitPostgresRow(internId, "Grace"),
				"the db-adapter should have written the record without being redeployed");
		assertTrue(awaitCsvLine(internId),
				"the file-adapter should have written the record without being redeployed");
	}

	/**
	 * Polls rather than sleeps-then-asserts. The pipeline is asynchronous by
	 * design, so a fixed wait would either be slower than it needs to be or
	 * flaky on a loaded machine -- and this suite runs on both.
	 */
	private static boolean awaitPostgresRow(String internId, String expectedFirstName) throws Exception {
		String jdbcUrl = "jdbc:postgresql://" + postgres.getHost() + ":" + postgres.getMappedPort(5432) + "/iip";
		for (int i = 0; i < 30; i++) {
			Thread.sleep(1000);
			try (Connection conn = DriverManager.getConnection(jdbcUrl, "iip", "iip");
					PreparedStatement stmt = conn.prepareStatement(
							"SELECT first_name FROM interns WHERE intern_id = ?")) {
				stmt.setString(1, internId);
				try (ResultSet rs = stmt.executeQuery()) {
					if (rs.next()) {
						assertEquals(expectedFirstName, rs.getString("first_name"));
						return true;
					}
				}
			}
		}
		return false;
	}

	private static boolean awaitCsvLine(String internId) throws Exception {
		for (int i = 0; i < 30; i++) {
			Thread.sleep(1000);
			if (fileAdapter.execInContainer("cat", "/data/interns.csv").getStdout().contains(internId)) {
				return true;
			}
		}
		return false;
	}

	private static String sourceServiceUrl() {
		return "http://" + sourceService.getHost() + ":" + sourceService.getMappedPort(8080);
	}

	private static String send(HttpClient client, HttpRequest.Builder request) throws Exception {
		return client.send(request.build(), HttpResponse.BodyHandlers.ofString()).body();
	}

	private Container.ExecResult exec(String command) throws Exception {
		return toolbox.execInContainer("sh", "-c", command);
	}
}
