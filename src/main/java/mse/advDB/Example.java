package mse.advDB;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.neo4j.driver.*;

import java.io.*;
import java.net.URL;
import java.net.URLConnection;
import java.util.*;

import static org.neo4j.driver.Values.parameters;

public class Example {

	private static final String QUERY = "UNWIND $batch AS row " + "MERGE (a:ARTICLE { _id: row.id }) "
			+ "SET a.title = row.title " +

			"WITH a, row " + "UNWIND row.authors AS author " + "MERGE (au:AUTHOR { _id: author.id }) "
			+ "SET au.name = author.name " + "MERGE (au)-[:AUTHORED]->(a) " +

			"WITH a, row " + "UNWIND row.citations AS refId " + "MERGE (ref:ARTICLE { _id: refId }) "
			+ "MERGE (a)-[:CITES]->(ref)";

	public static void main(String[] args) throws Exception {

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

		ObjectMapper mapper = new ObjectMapper();

		List<Map<String, Object>> batch = new ArrayList<>();
		int count = 0;
		int txCounter = 0;

		long startTime = System.currentTimeMillis();
		long startTimeBatch = startTime;

		try (Session session = driver.session()) {

			Transaction tx = session.beginTransaction();

			if (jsonPath.startsWith("http")) {

				long bytesRead = 0;

				while (true) {
					try {
						URLConnection conn = new URL(jsonPath).openConnection();
						conn.setConnectTimeout(10000);
						conn.setReadTimeout(60000);
						conn.setRequestProperty("Range", "bytes=" + bytesRead + "-");

						InputStream is = conn.getInputStream();
						BufferedInputStream bis = new BufferedInputStream(is);

						StringBuilder lineBuilder = new StringBuilder();
						boolean skipFirstLine = (bytesRead > 0);

						int b;

						while ((b = bis.read()) != -1) {
							bytesRead++;

							if (b == '\n') {

								if (skipFirstLine) {
									lineBuilder.setLength(0);
									skipFirstLine = false;
									continue;
								}

								String line = lineBuilder.toString();
								lineBuilder.setLength(0);

								count = processLine(line, mapper, batch, session, tx, batchSize, count, txCounter,
										startTimeBatch, startTime);

								if (batch.size() >= batchSize) {
									tx.run(QUERY, parameters("batch", batch));
									batch.clear();
									tx.commit();
									tx = session.beginTransaction();

									txCounter++;

									long now = System.currentTimeMillis();

									int elapsedBatch = (int) ((now - startTimeBatch) / 1000);
									int elapsedTotal = (int) ((now - startTime) / 1000);

									String elapsedTotalHuman = String.format("%02d:%02d:%02d", elapsedTotal / 3600,
											(elapsedTotal % 3600) / 60, elapsedTotal % 60);

									System.out.println(
											txCounter + "," + count + "," + elapsedBatch + "," + elapsedTotalHuman);

									startTimeBatch = now;
								}

								if (maxNodes != -1 && count >= maxNodes)
									break;
							} else {
								lineBuilder.append((char) b);
							}
						}

						bis.close();
						break;

					} catch (IOException e) {
						System.err.println("Connection lost. Resuming at byte " + bytesRead);
						Thread.sleep(3000);
					}
				}

			} else {
				// LOCAL FILE (simpler)
				BufferedReader br = new BufferedReader(new FileReader(jsonPath));
				String line;

				while ((line = br.readLine()) != null && (maxNodes != -1 ? count < maxNodes : true)) {

					count = processLine(line, mapper, batch, session, tx, batchSize, count, txCounter, startTimeBatch,
							startTime);

					if (batch.size() >= batchSize) {
						tx.run(QUERY, parameters("batch", batch));
						batch.clear();
						tx.commit();
						tx = session.beginTransaction();

						txCounter++;
					}
				}

				br.close();
			}

			if (!batch.isEmpty()) {
				tx.run(QUERY, parameters("batch", batch));
			}

			tx.commit();
		}

		driver.close();

		long duration = (System.currentTimeMillis() - startTime) / 1000;
		System.out.println("Finished. Inserted " + count + " articles in " + duration + " seconds.");
	}

	private static int processLine(String line, ObjectMapper mapper, List<Map<String, Object>> batch, Session session,
			Transaction tx, int batchSize, int count, int txCounter, long startTimeBatch, long startTime) {

		try {
			JsonNode json = mapper.readTree(line);

			String id = json.has("id") ? json.get("id").asText() : null;
			if (id == null || id.isEmpty())
				return count;

			String title = json.has("title") ? json.get("title").asText() : "";

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
			return count + 1;

		} catch (Exception e) {
			System.err.println("Error processing line: " + e.getMessage());
			return count;
		}
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