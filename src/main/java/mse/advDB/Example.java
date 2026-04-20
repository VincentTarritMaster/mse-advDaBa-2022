package mse.advDB;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.driver.*;

import java.io.*;
import java.net.URL;
import java.util.*;

import static org.neo4j.driver.Values.parameters;

public class Example {

	private static final String QUERY = "UNWIND $batch AS row " +

			"MERGE (a:ARTICLE { _id: row.id }) " + "SET a.title = row.title " +

			"WITH a, row " +

			"UNWIND row.authors AS author " + "MERGE (au:AUTHOR { _id: author.id }) " + "SET au.name = author.name "
			+ "MERGE (au)-[:AUTHORED]->(a) " +

			"WITH a, row " +

			"UNWIND row.citations AS refId " + "MERGE (ref:ARTICLE { _id: refId }) " + "MERGE (a)-[:CITES]->(ref)";

	public static void main(String[] args) throws IOException, InterruptedException {

		String jsonPath = System.getenv("JSON_FILE");
		int maxNodes = System.getenv("MAX_NODES") != null ? Integer.parseInt(System.getenv("MAX_NODES")) : -1;
		String neo4jIP = System.getenv("NEO4J_IP");
		int batchSize = Integer.max(1000, Integer.parseInt(System.getenv("BATCH_SIZE")));

		System.out.println("JSON: " + jsonPath);
		System.out.println("Max nodes: " + maxNodes);
		System.out.println("Neo4j IP: " + neo4jIP);
		System.out.println("Batch size: " + batchSize);

		Driver driver = GraphDatabase.driver("bolt://" + neo4jIP + ":7687", AuthTokens.basic("neo4j", "test"));

		// Wait for Neo4j
		while (true) {
			try {
				System.out.println("Waiting for Neo4j...");
				Thread.sleep(3000);
				driver.verifyConnectivity();
				break;
			} catch (Exception ignored) {
			}
		}

		createConstraints(driver);

		BufferedReader br;
		if (jsonPath.startsWith("http")) {
			br = new BufferedReader(new InputStreamReader(new BufferedInputStream(new URL(jsonPath).openStream())));
		} else {
			br = new BufferedReader(new FileReader(jsonPath));
		}

		ObjectMapper mapper = new ObjectMapper();

		List<Map<String, Object>> batch = new ArrayList<>();
		int count = 0;
		int txCounter = 0;

		long startTime = System.currentTimeMillis();

		try (Session session = driver.session()) {

			Transaction tx = session.beginTransaction();

			String line;

			long startTimeBatch = System.currentTimeMillis();

			while ((line = br.readLine()) != null && (maxNodes != -1 ? count < maxNodes : true)) {

				try {
					JsonNode json = mapper.readTree(line);

					String id = json.has("id") ? json.get("id").asText() : null;
					if (id == null || id.isEmpty())
						continue;

					String title = json.has("title") ? json.get("title").asText() : "";

					// ✅ FIXED: authors as MAP (id + name)
					List<Map<String, String>> authors = new ArrayList<>();
					if (json.has("authors")) {
						for (JsonNode a : json.get("authors")) {
							if (a.has("id") && a.has("name")) {

								String authorId = a.get("id").asText();
								String name = a.get("name").asText();

								if (authorId == null || authorId.isEmpty())
									continue;

								Map<String, String> author = new HashMap<>();
								author.put("id", authorId);
								author.put("name", name);

								authors.add(author);
							}
						}
					}

					List<String> citations = new ArrayList<>();
					if (json.has("references")) {
						for (JsonNode ref : json.get("references")) {
							citations.add(ref.asText());
						}
					}

					Map<String, Object> record = new HashMap<>();
					record.put("id", id);
					record.put("title", title);
					record.put("authors", authors);
					record.put("citations", citations);

					batch.add(record);
					count++;

					if (batch.size() >= batchSize) {
						tx.run(QUERY, parameters("batch", batch));
						batch.clear();
						txCounter++;

						tx.commit();
						tx = session.beginTransaction();

						long now = System.currentTimeMillis();

						int elapsedBatch = (int) ((now - startTimeBatch) / 1000);
						int elapsedTotal = (int) ((now - startTime) / 1000);

						String elapsedTotalHuman = String.format("%02d:%02d:%02d", elapsedTotal / 3600,
								(elapsedTotal % 3600) / 60, elapsedTotal % 60);

						System.out.println(txCounter + "," + count + "," + elapsedBatch + "," + elapsedTotalHuman);

						startTimeBatch = now;
					}

				} catch (Exception e) {
					System.err.println("Error processing line: " + e.getMessage());
				}
			}

			if (!batch.isEmpty()) {
				tx.run(QUERY, parameters("batch", batch));
			}

			tx.commit();
		}

		br.close();
		driver.close();

		long duration = (System.currentTimeMillis() - startTime) / 1000;
		System.out.println("Finished. Inserted " + count + " articles in " + duration + " seconds.");
	}

	private static void createConstraints(Driver driver) {
		try (Session session = driver.session()) {
			session.writeTransaction(tx -> {

				tx.run("CREATE CONSTRAINT article_id IF NOT EXISTS " + "FOR (a:ARTICLE) REQUIRE a._id IS UNIQUE");

				tx.run("CREATE CONSTRAINT author_id IF NOT EXISTS " + "FOR (a:AUTHOR) REQUIRE a._id IS UNIQUE");

				return null;
			});

			System.out.println("Constraints ensured.");

		} catch (Exception e) {
			System.err.println("Constraint creation failed: " + e.getMessage());
		}
	}
}