/* Copyright (c) 2014 Stanford University
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package org.ellitron.neo4j.benchmarks;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.text.*;

import javax.ws.rs.core.MediaType;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.examples.server.Relation;
import org.neo4j.examples.server.TraversalDefinition;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.net.httpserver.*;

/**
 * HttpClientBenchmarks is the class encapsulating all performance benchmarks
 * for Neo4j run as a remote HTTP client, and related methods.
 * 
 * @author Jonathan Ellithorpe
 */
public class HttpClientBenchmarks {
	/**
	 * Stores the root URI for all requests to the database.
	 */
	private String server_root_uri;
	
	/**
	 * Constructor.
	 * 
	 * @param server_root_uri
	 *            The root URI for all requests to the database (e.g.
	 *            "http://192.168.1.1:8182/graphs/mygraph/").
	 */
	public HttpClientBenchmarks(String server_root_uri) {
		this.server_root_uri = server_root_uri;
	}
	
	/**
	 * Clears the database of all edges and nodes
	 */
	public void clearDatabase() {
		System.out.print("Clearing the database... ");
		
		// Clear edges
		String cypherQuery = "start r=relationship(*) delete r";
		WebResource resource = Client.create()
				.resource( server_root_uri + "cypher" );
		ClientResponse response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( "{ \"query\" : \"" + cypherQuery + "\" }" )
				.post( ClientResponse.class );

		response.close();

		// Clear nodes
		cypherQuery = "start n=node(*) delete n";
		resource = Client.create()
				.resource( server_root_uri + "cypher" );
		response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( "{ \"query\" : \"" + cypherQuery + "\" }" )
				.post( ClientResponse.class );

		response.close();
		
		System.out.println("done.");
	}
	
	/**
	 * Dumps latency measurements to a file.
	 * 
	 * @param benchmarkName
	 *            Name to give the benchmark.
	 * @param benchmarkSpec
	 *            Description of benchmark parameters to include in output
	 *            filename.
	 * @param timings
	 *            The timing data.
	 */
	public void dumpLatencyMeasurements(String benchmarkName, String benchmarkSpec, double timings[]) {
		System.out.print("Dumping latency measurements... ");
		
		try {
			String dateString = new SimpleDateFormat("yyyyMMdd'_'HHmmss").format(new Date());
			String filename = dateString + "_" + benchmarkName + "_" + benchmarkSpec + ".out";
			BufferedWriter br = new BufferedWriter(new FileWriter(filename));
			for(int i = 0; i<timings.length; i++) {
				br.write(String.format("%.6f\n", timings[i]));
			}
			br.flush();
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		System.out.println("done.");
	}
	
	/**
	 * Warms up the database with reads.
	 * 
	 * @param numReads
	 *            The number of reads to do for warm-up.
	 */
	public void warmUpDatabase(final int numReads) {
		System.out.print("Warming up the database... ");
		
		// Create a node
		WebResource resource = Client.create()
				.resource( server_root_uri + "node" );

		ClientResponse response = resource
				.accept( MediaType.APPLICATION_JSON )
				.post( ClientResponse.class );

		String jsonString = response.getEntity( String.class );
		JSONObject jsonObject = new JSONObject(jsonString);
		String nodeURIArray = jsonObject.getString("self");

		response.close();

		resource = Client.create()
				.resource( nodeURIArray );
		
		double[] timings = new double[numReads];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		for(int i = 0; i < numReads; i++) {
			long startTime = System.nanoTime();
			response = resource
					.accept( MediaType.APPLICATION_JSON)
					.get( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);
			
			response.close();
		}

		System.out.println("done.");
		
		System.out.println("Timings statistics:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numReads, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("warmUpDatabase", "numSamples=" + numReads, timings);
		
		clearDatabase();
	}
	
	/**
	 * Creates a set of nodes and reads them sequentially, measuring the latency
	 * of each read.
	 * 
	 * @param numSamples
	 *            The number of nodes to create and then read sequentially.
	 */
	public void run01(final int numSamples) {
		System.out.println("Running Benchmark 01... Reading nodes");
		System.out.println("\tnumSamples:\t" + numSamples);
		
		// Create numSamples nodes and collect their IDs
		WebResource resource = Client.create()
				.resource( server_root_uri + "node" );

		String[] nodeURIArray = new String[numSamples];

		for(int i = 0; i < numSamples; i++) {
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject jsonObject = new JSONObject(jsonString);
			nodeURIArray[i] = jsonObject.getString("self");

			response.close();
		}
		
		// Read nodes sequentially
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();

		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( nodeURIArray[i] );
			
			long startTime = System.nanoTime();
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON)
					.get( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

			response.close();
		}

		System.out.println("Timings statistics:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark01", "numSamples=" + numSamples, timings);

		clearDatabase();
	}

	/**
	 * Creates a set of edges and reads them sequentially, measuring the latency
	 * of each read.
	 * 
	 * @param numSamples
	 *            The number of edges to create and then read sequentially.
	 */
	public void run02(final int numSamples) {
		System.out.println("Running Benchmark 02... Reading edges");
		System.out.println("\tnumSamples:\t" + numSamples);

		// Create 2*numSamples nodes and collect their IDs
		WebResource resource = Client.create()
				.resource( server_root_uri + "node" );

		String[] nodeURIArray = new String[2*numSamples];
		
		for(int i = 0; i < 2*numSamples; i++) {
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject jsonObject = new JSONObject(jsonString);
			nodeURIArray[i] = jsonObject.getString("self");

			response.close();
		}
		
//		for(int i = 0; i < 2*numSamples; i++)
//			System.out.println("nodeURI[" + i + "]: " + nodeURIArray[i]);
		
		// Create numSamples edges
		String[] edgeURIArray = new String[numSamples];
		
		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( nodeURIArray[i] + "/relationships" );
			
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"to\" : \"" + nodeURIArray[i+numSamples] + "\", \"type\" : \"KNOWS\" }" )
					.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject jsonObject = new JSONObject(jsonString);
			edgeURIArray[i] = jsonObject.getString("self");
			
			response.close();
		}
		
//		for(int i = 0; i < numSamples; i++)
//			System.out.println("edgeURI[" + i + "]: " + edgeURIArray[i]);
		
		// Read edges sequentially
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( edgeURIArray[i] );
			
			long startTime = System.nanoTime();
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.get( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

//			String jsonString = response.getEntity( String.class );
//			System.out.println("response: " + jsonString);
			
			response.close();
		}
		
		System.out.println("Timings statistics:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());
		
		dumpLatencyMeasurements("benchmark02", "numSamples=" + numSamples, timings);
		
		clearDatabase();
	}
	
	public void run03() {
		
	}
	
	public void run04() {
		
	}
	
	/**
	 * Creates a set of nodes, measuring the latency of each create.
	 * 
	 * @param numSamples
	 *            The number of nodes to create.
	 */
	public void run05(final int numSamples) {
		System.out.println("Running Benchmark 05... Creating nodes");
		System.out.println("\tnumSamples:\t" + numSamples);

		WebResource resource = Client.create()
				.resource( server_root_uri + "node" );
		
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		for(int i = 0; i < numSamples; i++) {
			long startTime = System.nanoTime();
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON)
					.post( ClientResponse.class );
			long endTime = System.nanoTime();
			
			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);
			
			response.close();
		}
		
		System.out.println("Timings statistics:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());
		
		dumpLatencyMeasurements("benchmark05", "numSamples=" + numSamples, timings);
		
		clearDatabase();
	}
	
	/**
	 * Creates a set of edges, measuring the latency of each create.
	 * 
	 * @param numSamples
	 *            The number of edges to create.
	 */
	public void run06(final int numSamples) {
		System.out.println("Running Benchmark 06... Creating edges");
		System.out.println("\tnumSamples:\t" + numSamples);

		// Create 2*numSamples nodes and collect their IDs
		WebResource resource = Client.create()
				.resource( server_root_uri + "node" );

		String[] nodeURIArray = new String[2*numSamples];
		
		for(int i = 0; i < 2*numSamples; i++) {
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject jsonObject = new JSONObject(jsonString);
			nodeURIArray[i] = jsonObject.getString("self");

			response.close();
		}
		
		// Create numSample edges
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( nodeURIArray[i] + "/relationships" );
			
			long startTime = System.nanoTime();
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"to\" : \"" + nodeURIArray[i+numSamples] + "\", \"type\" : \"KNOWS\" }" )
					.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

			response.close();
		}
		
		System.out.println("Timings statistics:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());
		
		dumpLatencyMeasurements("benchmark06", "numSamples=" + numSamples, timings);
		
		clearDatabase();
	}
	
	/**
	 * Creates a set of nodes with property "prop" set to 42, then updates the
	 * node properties to 43 sequentially, measuring the latency of each update.
	 * 
	 * @param numSamples
	 *            The number of nodes to create and then update sequentially.
	 */
	public void run07(final int numSamples) {
		System.out.println("Running Benchmark 07... Updating node properties");
		System.out.println("\tnumSamples:\t" + numSamples);

		// Create numSamples nodes with properties and collect their IDs
		WebResource resource = Client.create()
				.resource( server_root_uri + "node" );

		String[] nodeURIArray = new String[numSamples];

		for(int i = 0; i < numSamples; i++) {
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"prop\" : 42 }" )
					.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject jsonObject = new JSONObject(jsonString);
			nodeURIArray[i] = jsonObject.getString("self");

			response.close();
		}
		
//		for(int i = 0; i < numSamples; i++)
//			System.out.println("nodeURI[" + i + "]: " + nodeURIArray[i]);
		
		// Update node properties
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( nodeURIArray[i] + "/properties" );
			
			long startTime = System.nanoTime();
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"prop\" : 43 }" )
					.put( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

			response.close();
		}
		
		System.out.println("Timings statistics:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());
		
		dumpLatencyMeasurements("benchmark07", "numSamples=" + numSamples, timings);
		
		clearDatabase();
	}
	
	/**
	 * Creates a set of edges with property "prop" set to 42, then updates the
	 * edge properties to 43 sequentially, measuring the latency of each update.
	 * 
	 * @param numSamples
	 *            The number of edges to create and then update sequentially.
	 */
	public void run08(final int numSamples) {
		System.out.println("Running Benchmark 08... Updating edge properties");
		System.out.println("\tnumSamples:\t" + numSamples);
		
		// Create 2*numSamples nodes and collect their IDs
		WebResource resource = Client.create()
				.resource( server_root_uri + "node" );

		String[] nodeURIArray = new String[2*numSamples];

		for(int i = 0; i < 2*numSamples; i++) {
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject jsonObject = new JSONObject(jsonString);
			nodeURIArray[i] = jsonObject.getString("self");

			response.close();
		}

//		for(int i = 0; i < 2*numSamples; i++)
//			System.out.println("nodeURI[" + i + "]: " + nodeURIArray[i]);

		// Create numSample edges with properties
		String[] edgeURIArray = new String[numSamples];

		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( nodeURIArray[i] + "/relationships" );

			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"to\" : \"" + nodeURIArray[i+numSamples] + "\", \"type\" : \"KNOWS\", \"data\" : { \"prop\" : 42 } }" )
					.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject jsonObject = new JSONObject(jsonString);
			edgeURIArray[i] = jsonObject.getString("self");

			response.close();
		}

//		for(int i = 0; i < numSamples; i++)
//			System.out.println("edgeURI[" + i + "]: " + edgeURIArray[i]);

		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();

		// Update properties on edges
		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( edgeURIArray[i] + "/properties" );
			
			long startTime = System.nanoTime();
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"prop\" : 43 }" )
					.put( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

			response.close();
		}

		System.out.println("Timings statistics:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark08", "numSamples=" + numSamples, timings);

		clearDatabase();
	}
	
	/**
	 * Creates a set of nodes and deletes them sequentially, measuring the
	 * latency of each delete.
	 * 
	 * @param numSamples
	 *            The number of nodes to create and then delete sequentially.
	 */
	public void run09(final int numSamples) {
		System.out.println("Running Benchmark 09... Deleting nodes");
		System.out.println("\tnumSamples:\t" + numSamples);

		// Create numSamples nodes and collect their IDs
		WebResource resource = Client.create()
				.resource( server_root_uri + "node" );

		String[] nodeURIArray = new String[numSamples];

		for(int i = 0; i < numSamples; i++) {
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject jsonObject = new JSONObject(jsonString);
			nodeURIArray[i] = jsonObject.getString("self");

			response.close();
		}
		
		// Now delete them all!
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();

		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( nodeURIArray[i] );
			
			long startTime = System.nanoTime();
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON)
					.delete( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

			response.close();
		}

		System.out.println("Timings statistics:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark09", "numSamples=" + numSamples, timings);

		clearDatabase();
	}
	
	/**
	 * Creates a set of edges and deletes them sequentially, measuring the
	 * latency of each delete.
	 * 
	 * @param numSamples
	 *            The number of edges to create and then delete sequentially.
	 */
	public void run10(final int numSamples) {
		System.out.println("Running Benchmark 10... Deleting edges");
		System.out.println("\tnumSamples:\t" + numSamples);

		// Create 2*numSamples nodes and collect their IDs
		WebResource resource = Client.create()
				.resource( server_root_uri + "node" );

		String[] nodeURIArray = new String[2*numSamples];
		
		for(int i = 0; i < 2*numSamples; i++) {
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject jsonObject = new JSONObject(jsonString);
			nodeURIArray[i] = jsonObject.getString("self");

			response.close();
		}
		
//		for(int i = 0; i < 2*numSamples; i++)
//			System.out.println("nodeURI[" + i + "]: " + nodeURIArray[i]);
		
		// Create numSample edges
		String[] edgeURIArray = new String[numSamples];
		
		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( nodeURIArray[i] + "/relationships" );
			
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"to\" : \"" + nodeURIArray[i+numSamples] + "\", \"type\" : \"KNOWS\" }" )
					.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject jsonObject = new JSONObject(jsonString);
			edgeURIArray[i] = jsonObject.getString("self");
			
			response.close();
		}
		
//		for(int i = 0; i < numSamples; i++)
//			System.out.println("edgeURI[" + i + "]: " + edgeURIArray[i]);
		
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		// Delete numSamples edges
		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( edgeURIArray[i] );
			
			long startTime = System.nanoTime();
			ClientResponse response = resource
					.accept( MediaType.APPLICATION_JSON )
					.delete( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

//			String jsonString = response.getEntity( String.class );
//			System.out.println("response: " + jsonString);
			
			response.close();
		}
		
		System.out.println("Timings statistics:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());
		
		dumpLatencyMeasurements("benchmark10", "numSamples=" + numSamples, timings);
		
		clearDatabase();
	}
	
	/**
	 * Creates, reads, updates, and deletes a set of nodes with a single
	 * property set. Read, update, and delete operations are performed using the
	 * (un-indexed) property value as a look-up. Latency measurements are taken
	 * for each operation.
	 * <p>
	 * All operations are performed using Cypher queries sent to the server's
	 * Cypher query processing engine.
	 * 
	 * @param numSamples
	 *            The number of nodes to create, read, update, and then delete
	 *            sequentially.
	 */
	public void run11(final int numSamples) {
		System.out.println("Running Benchmark 11... Timing CRUD without indices");
		System.out.println("\tnumSamples:\t" + numSamples);

		WebResource resource = Client.create()
				.resource( server_root_uri + "cypher" );
		ClientResponse response;
		
		// Create nodes
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		String createNodeCQ = "create (:Person{ID:{id}});";
		
		for(int id = 0; id<numSamples; id++) {
			String createNodeParams = String.format("\"id\" : %d", id);
			long startTime = System.nanoTime();
			response = resource.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"query\" : \"" + createNodeCQ + "\", \"params\" : { " + createNodeParams + " } }" )
					.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);
			
//			String jsonString = response.getEntity(String.class);
//			System.out.println("create response:\n" + jsonString);
			
			response.close();
		}
		
		System.out.println("Timings statistics for create node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());
	
		dumpLatencyMeasurements("benchmark11create", "numSamples=" + numSamples, timings);
		
		// Read nodes
		sumStats = new SummaryStatistics();
		
		String readNodeCQ = "match (n:Person) where n.ID={id} return n;";

		for(int id = 0; id<numSamples; id++) {
			String readNodeParams = String.format("\"id\" : %d", id);
			long startTime = System.nanoTime();
			response = resource.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"query\" : \"" + readNodeCQ + "\", \"params\" : { " + readNodeParams + " } }" )
					.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);
			
//			String jsonString = response.getEntity(String.class);
//			System.out.println("read response:\n" + jsonString);
			
			response.close();
		}

		System.out.println("Timings statistics for read node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark11read", "numSamples=" + numSamples, timings);
		
		// Update property of the nodes
		sumStats = new SummaryStatistics();
		
		String updateNodeCQ = "match (n:Person) where n.ID={id} set n.ID={newId};";

		for(int id = 0; id<numSamples; id++) {
			String updateNodeParams = String.format("\"id\" : %d, \"newId\" : %d", id, id+numSamples);
			long startTime = System.nanoTime();
			response = resource.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"query\" : \"" + updateNodeCQ + "\", \"params\" : { " + updateNodeParams + " } }" )
					.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);

//			String jsonString = response.getEntity(String.class);
//			System.out.println("update response:\n" + jsonString);
			
			response.close();
		}

		System.out.println("Timings statistics for update node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark11update", "numSamples=" + numSamples, timings);
		
		// Delete nodes
		sumStats = new SummaryStatistics();
		
		String deleteNodeCQ = "match (n:Person) where n.ID={id} delete n;";

		for(int id = 0; id<numSamples; id++) {
			String deleteNodeParams = String.format("\"id\" : %d", id);
			long startTime = System.nanoTime();
			response = resource.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"query\" : \"" + deleteNodeCQ + "\", \"params\" : { " + deleteNodeParams + " } }" )
					.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);

//			String jsonString = response.getEntity(String.class);
//			System.out.println("delete response:\n" + jsonString);
			
			response.close();
		}

		System.out.println("Timings statistics for delete node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark11delete", "numSamples=" + numSamples, timings);
		
		clearDatabase();
	}
	
	/**
	 * Creates, reads, updates, and deletes a set of nodes with a single
	 * property set for which an index has been created. Read, update, and
	 * delete operations are performed using the property value as a look-up.
	 * Latency measurements are taken for each operation.
	 * <p>
	 * All operations are performed using Cypher queries sent to the server's
	 * Cypher query processing engine.
	 * 
	 * @param numSamples
	 *            The number of nodes to create, read, update, and then delete
	 *            sequentially.
	 */
	public void run12(final int numSamples) {
		System.out.println("Running Benchmark 12... Timing CRUD with indices");
		System.out.println("\tnumSamples:\t" + numSamples);

		WebResource resource;
		ClientResponse response;
		
		// Create an index
		String createIndexCQ = "create index on :Person(ID);";
		
		resource = Client.create()
				.resource( server_root_uri + "cypher" );
		response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( "{ \"query\" : \"" + createIndexCQ + "\" }" )
				.post( ClientResponse.class );
		
		response.close();
		
		// Wait a bit for the index to become ready.
		try {
		    Thread.sleep(1000);
		} catch(InterruptedException ex) {
		    Thread.currentThread().interrupt();
		}
		
		// Create nodes in the index
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		String createNodeCQ = "create (:Person{ID:{id}});";
		
		for(int id = 0; id<numSamples; id++) {
			String createNodeParams = String.format("\"id\" : %d", id);
			long startTime = System.nanoTime();
			response = resource.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"query\" : \"" + createNodeCQ + "\", \"params\" : { " + createNodeParams + " } }" )
					.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);
			
//			String jsonString = response.getEntity(String.class);
//			System.out.println("create response:\n" + jsonString);
			
			response.close();
		}
		
		System.out.println("Timings statistics for create node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());
	
		dumpLatencyMeasurements("benchmark12create", "numSamples=" + numSamples, timings);
		
		// Read nodes from the index
		sumStats = new SummaryStatistics();
		
		String readNodeCQ = "match (n:Person) where n.ID={id} return n;";

		for(int id = 0; id<numSamples; id++) {
			String readNodeParams = String.format("\"id\" : %d", id);
			long startTime = System.nanoTime();
			response = resource.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"query\" : \"" + readNodeCQ + "\", \"params\" : { " + readNodeParams + " } }" )
					.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);
			
//			String jsonString = response.getEntity(String.class);
//			System.out.println("read response:\n" + jsonString);
			
			response.close();
		}

		System.out.println("Timings statistics for read node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark12read", "numSamples=" + numSamples, timings);
		
		// Update indexed property of the nodes
		sumStats = new SummaryStatistics();
		
		String updateNodeCQ = "match (n:Person) where n.ID={id} set n.ID={newId};";

		for(int id = 0; id<numSamples; id++) {
			String updateNodeParams = String.format("\"id\" : %d, \"newId\" : %d", id, id+numSamples);
			long startTime = System.nanoTime();
			response = resource.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"query\" : \"" + updateNodeCQ + "\", \"params\" : { " + updateNodeParams + " } }" )
					.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);

//			String jsonString = response.getEntity(String.class);
//			System.out.println("update response:\n" + jsonString);
			
			response.close();
		}

		System.out.println("Timings statistics for update node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark12update", "numSamples=" + numSamples, timings);
		
		// Delete nodes in the index
		sumStats = new SummaryStatistics();
		
		String deleteNodeCQ = "match (n:Person) where n.ID={id} delete n;";

		for(int id = 0; id<numSamples; id++) {
			String deleteNodeParams = String.format("\"id\" : %d", id);
			long startTime = System.nanoTime();
			response = resource.accept( MediaType.APPLICATION_JSON )
					.type( MediaType.APPLICATION_JSON )
					.entity( "{ \"query\" : \"" + deleteNodeCQ + "\", \"params\" : { " + deleteNodeParams + " } }" )
					.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);

//			String jsonString = response.getEntity(String.class);
//			System.out.println("delete response:\n" + jsonString);
			
			response.close();
		}

		System.out.println("Timings statistics for delete node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark12delete", "numSamples=" + numSamples, timings);
		
		// Drop the index
		String dropIndexCQ = "drop index on :Person(ID);";
		
		response = resource.accept( MediaType.APPLICATION_JSON )
				.type( MediaType.APPLICATION_JSON )
				.entity( "{ \"query\" : \"" + dropIndexCQ + "\" }" )
				.post( ClientResponse.class );
		
		response.close();
		
		clearDatabase();
	}
}
