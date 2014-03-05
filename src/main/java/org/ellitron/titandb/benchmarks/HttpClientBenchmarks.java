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

package org.ellitron.titandb.benchmarks;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.ws.rs.core.MediaType;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.json.JSONArray;
import org.json.JSONObject;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

/**
 * HttpClientBenchmarks is the class encapsulating all performance benchmarks
 * for TitanDB run as a remote HTTP client, and related methods.
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
	 * Clears the database of all edges and nodes.
	 */
	public void clearDatabase() {
		System.out.print("Clearing the database... ");
		
		// Delete all edges
		WebResource resource = Client.create()
				.resource( server_root_uri + "tp/gremlin?script=g.E.remove()" );

		ClientResponse response = resource.get( ClientResponse.class );

		response.close();
				
		// Delete all vertices
		resource = Client.create()
				.resource( server_root_uri + "tp/gremlin?script=g.V.remove()" );

		response = resource.get( ClientResponse.class );
		
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
			// TODO Auto-generated catch block
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
		
		// Create a vertex
		WebResource resource = Client.create()
				.resource( server_root_uri + "vertices/" );

		ClientResponse response = resource.post( ClientResponse.class );

		String jsonString = response.getEntity( String.class );
		JSONObject queryRetVal = new JSONObject( jsonString );
		JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
		long vId = resultsObject.getLong("_id");

		response.close();

		// Read the vertex
		resource = Client.create()
				.resource( server_root_uri + "vertices/" + vId );

		double[] timings = new double[numReads];
		SummaryStatistics sumStats = new SummaryStatistics();

		for(int i = 0; i < numReads; i++) {
			long startTime = System.nanoTime();
			response = resource.get( ClientResponse.class );
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

		// Create numSamples vertices
		WebResource resource = Client.create()
				.resource( server_root_uri + "vertices/" );

		long[] vIdArray = new long[numSamples];

		for(int i = 0; i < numSamples; i++) {
			ClientResponse response = resource.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			vIdArray[i] = resultsObject.getLong("_id");

			response.close();
		}

		// Read the vertices
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();

		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( server_root_uri + "vertices/" + vIdArray[i] );
			
			long startTime = System.nanoTime();
			ClientResponse response = resource.get( ClientResponse.class );
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
		System.out.println("Running Benchmark 02... Reading edgds");
		System.out.println("\tnumSamples:\t" + numSamples);

		// Create 2*numSamples vertices
		WebResource resource = Client.create()
				.resource( server_root_uri + "vertices/" );

		long[] vIdArray = new long[2*numSamples];

		for(int i = 0; i < 2*numSamples; i++) {
			ClientResponse response = resource.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			vIdArray[i] = resultsObject.getLong("_id");

			response.close();
		}

		// Create numSamples edges
		String[] eIdArray = new String[numSamples];

		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( server_root_uri + "edges?_outV=" + vIdArray[i] + "&_label=friend&_inV=" + vIdArray[i+numSamples] );

			ClientResponse response = resource.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			eIdArray[i] = resultsObject.getString("_id");

			response.close();
		}

		// Read numSamples edges
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();

		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( server_root_uri + "edges/" + eIdArray[i] );

			long startTime = System.nanoTime();
			ClientResponse response = resource.get( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

			response.close();
		}

		System.out.println("Timings statistics:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark02", "numSamples=" + numSamples, timings);

		clearDatabase();
	}
	
	public void run03(final int numSamples) {
		
	}
	
	public void run04(final int numSamples) {
		
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
				.resource( server_root_uri + "vertices/" );
		
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		for(int i = 0; i < numSamples; i++) {
			long startTime = System.nanoTime();
			ClientResponse response = resource.post( ClientResponse.class );
			long endTime = System.nanoTime();
			
			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);
			
//			String jsonString = response.getEntity( String.class );
//			System.out.println(jsonString);
			
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
		
		// Create 2*numSamples vertices
		WebResource resource = Client.create()
				.resource( server_root_uri + "vertices/" );
		
		long[] vIdArray = new long[2*numSamples];
		
		for(int i = 0; i < 2*numSamples; i++) {
			ClientResponse response = resource.post( ClientResponse.class );
			
			String jsonString = response.getEntity( String.class );
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			vIdArray[i] = resultsObject.getLong("_id");
			
			response.close();
		}
		
		// Create numSamples edges
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( server_root_uri + "edges?_outV=" + vIdArray[i] + "&_label=friend&_inV=" + vIdArray[i+numSamples] );
			
			long startTime = System.nanoTime();
			ClientResponse response = resource.post( ClientResponse.class );
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
		System.out.println("Running Benchmark 07... Updating nodes");
		System.out.println("\tnumSamples:\t" + numSamples);
		
		// First create numSamples nodes and collect their IDs
		WebResource resource = Client.create()
				.resource( server_root_uri + "vertices/" );

		long[] vIdArray = new long[numSamples];

		for(int i = 0; i < numSamples; i++) {
			ClientResponse response = resource.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			vIdArray[i] = resultsObject.getLong("_id");

			response.close();
		}
		
		// Now create node properties.
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		for(int i = 0; i < numSamples; i++ ) {
			resource = Client.create()
					.resource( server_root_uri + "vertices/" + vIdArray[i] + "?prop=42" );

			long startTime = System.nanoTime();
			ClientResponse response = resource.post( ClientResponse.class );
			long endTime = System.nanoTime();
			
			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);
			
			response.close();
		}
		
		System.out.println("Timings statistics for creating properties:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		// Now update node properties.
		for(int i = 0; i < numSamples; i++ ) {
			resource = Client.create()
					.resource( server_root_uri + "vertices/" + vIdArray[i] + "?prop=43" );

			long startTime = System.nanoTime();
			ClientResponse response = resource.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

			response.close();
		}

		System.out.println("Timings statistics for updating properties:");
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
		System.out.println("Running Benchmark 08... Updating edges");
		System.out.println("\tnumSamples:\t" + numSamples);
		
		// Create 2*numSamples vertices
		WebResource resource = Client.create()
				.resource( server_root_uri + "vertices/" );

		long[] vIdArray = new long[2*numSamples];

		for(int i = 0; i < 2*numSamples; i++) {
			ClientResponse response = resource.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			vIdArray[i] = resultsObject.getLong("_id");

			response.close();
		}

		// Create numSamples edges
		String[] eIdArray = new String[numSamples];

		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( server_root_uri + "edges?_outV=" + vIdArray[i] + "&_label=friend&_inV=" + vIdArray[i+numSamples] );

			ClientResponse response = resource.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			eIdArray[i] = resultsObject.getString("_id");

			response.close();
		}
		
		// Now create edge properties.
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		for(int i = 0; i < numSamples; i++ ) {
			resource = Client.create()
					.resource( server_root_uri + "edges/" + eIdArray[i] + "?prop=42" );

			long startTime = System.nanoTime();
			ClientResponse response = resource.post( ClientResponse.class );
			long endTime = System.nanoTime();
			
			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);
			
			response.close();
		}
		
		System.out.println("Timings statistics for creating properties:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		// Now update node properties.
		for(int i = 0; i < numSamples; i++ ) {
			resource = Client.create()
					.resource( server_root_uri + "edges/" + eIdArray[i] + "?prop=43" );

			long startTime = System.nanoTime();
			ClientResponse response = resource.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

			response.close();
		}

		System.out.println("Timings statistics for updating properties:");
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
		
		// Create numSamples vertices
		WebResource root_resource = Client.create()
				.resource( server_root_uri + "vertices/" );

		long[] vIdArray = new long[numSamples];

		for(int i = 0; i < numSamples; i++) {
			ClientResponse response = root_resource.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			vIdArray[i] = resultsObject.getLong("_id");

			response.close();
		}

		// Delete numSamples vertices
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();

		for(int i = 0; i < numSamples; i++) {
			WebResource resource = root_resource.path( Long.toString(vIdArray[i]) );

			long startTime = System.nanoTime();
			ClientResponse response = resource.delete( ClientResponse.class );
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
		
		// Create 2*numSamples vertices
		WebResource resource = Client.create()
				.resource( server_root_uri + "vertices/" );

		long[] vIdArray = new long[2*numSamples];

		for(int i = 0; i < 2*numSamples; i++) {
			ClientResponse response = resource.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			vIdArray[i] = resultsObject.getLong("_id");

			response.close();
		}

		// Create numSamples edges
		String[] eIdArray = new String[numSamples];
		
		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( server_root_uri + "edges?_outV=" + vIdArray[i] + "&_label=friend&_inV=" + vIdArray[i+numSamples] );

			ClientResponse response = resource.post( ClientResponse.class );

			String jsonString = response.getEntity( String.class );
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			eIdArray[i] = resultsObject.getString("_id");

			response.close();
		}
		
		// Delete numSamples edges
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();

		for(int i = 0; i < numSamples; i++) {
			resource = Client.create()
					.resource( server_root_uri + "edges/" + eIdArray[i] );

			long startTime = System.nanoTime();
			ClientResponse response = resource.delete( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[i] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[i]);

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
	 * 
	 * @param numSamples
	 *            The number of nodes to create, read, update, and then delete
	 *            sequentially.
	 */
	public void run11(final int numSamples) {
		System.out.println("Running Benchmark 11... Timing CRUD without indices");
		System.out.println("\tnumSamples:\t" + numSamples);
		
		WebResource resource;
		ClientResponse response;
		
		// Create nodes in the index
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		// Collect IDs for later
		long[] vIdArray = new long[numSamples];
		
		for(int id = 0; id<numSamples; id++) {
			resource = Client.create()
					.resource( server_root_uri + "vertices?name=" + id );
			
			long startTime = System.nanoTime();
			response = resource.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);
			
			String jsonString = response.getEntity( String.class );
//			System.out.println("create response:\n" + jsonString);
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			vIdArray[id] = resultsObject.getLong("_id");
			
			response.close();
		}
		
		System.out.println("Timings statistics for create node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());
	
		dumpLatencyMeasurements("benchmark11create", "numSamples=" + numSamples, timings);
		
		// Read nodes from the index
		sumStats = new SummaryStatistics();
		
		for(int id = 0; id<numSamples; id++) {
			resource = Client.create()
					.resource( server_root_uri + "vertices?key=name&value=" + id );
			
			long startTime = System.nanoTime();
			response = resource.get( ClientResponse.class );
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
		
		// Update indexed property of the nodes
		sumStats = new SummaryStatistics();
		
		for(int id = 0; id<numSamples; id++) {
			resource = Client.create()
					.resource( server_root_uri + "vertices/" + vIdArray[id] + "?name=" + id+numSamples );
			
			long startTime = System.nanoTime();
			response = resource.post( ClientResponse.class );
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
		
		// Delete nodes in the index
		sumStats = new SummaryStatistics();
		
		for(int id = 0; id<numSamples; id++) {
			resource = Client.create()
					.resource( server_root_uri + "vertices/" + vIdArray[id] );
			
			long startTime = System.nanoTime();
			response = resource.delete( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);
			
//			String jsonString = response.getEntity( String.class );
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
		resource = Client.create()
				.resource( server_root_uri + "keyindices/vertex/name" );
		response = resource.post( ClientResponse.class );
		response.close();
		
		// Create nodes in the index
		double[] timings = new double[numSamples];
		SummaryStatistics sumStats = new SummaryStatistics();
		
		// Collect IDs for later
		long[] vIdArray = new long[numSamples];
		
		for(int id = 0; id<numSamples; id++) {
			resource = Client.create()
					.resource( server_root_uri + "vertices?name=" + id );
			
			long startTime = System.nanoTime();
			response = resource.post( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);
			
			String jsonString = response.getEntity( String.class );
//			System.out.println("create response:\n" + jsonString);
			JSONObject queryRetVal = new JSONObject( jsonString );
			JSONObject resultsObject = queryRetVal.getJSONObject( "results" );
			vIdArray[id] = resultsObject.getLong("_id");
			
			response.close();
		}
		
		System.out.println("Timings statistics for create node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());
	
		dumpLatencyMeasurements("benchmark12create", "numSamples=" + numSamples, timings);
		
		// Read nodes from the index
		sumStats = new SummaryStatistics();
		
		for(int id = 0; id<numSamples; id++) {
			resource = Client.create()
					.resource( server_root_uri + "vertices?key=name&value=" + id );
			
			long startTime = System.nanoTime();
			response = resource.get( ClientResponse.class );
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
		
		for(int id = 0; id<numSamples; id++) {
			resource = Client.create()
					.resource( server_root_uri + "vertices/" + vIdArray[id] + "?name=" + id+numSamples );
			
			long startTime = System.nanoTime();
			response = resource.post( ClientResponse.class );
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
		
		for(int id = 0; id<numSamples; id++) {
			resource = Client.create()
					.resource( server_root_uri + "vertices/" + vIdArray[id] );
			
			long startTime = System.nanoTime();
			response = resource.delete( ClientResponse.class );
			long endTime = System.nanoTime();

			timings[id] = (endTime - startTime)/1e6; // Timings recorded in ms
			sumStats.addValue(timings[id]);
			
//			String jsonString = response.getEntity( String.class );
//			System.out.println("delete response:\n" + jsonString);
			
			response.close();
		}
		
		System.out.println("Timings statistics for delete node:");
		System.out.printf("numSamples: %d min: %11.6f max: %11.6f mean: %11.6f stdDev: %11.6f\n", numSamples, sumStats.getMin(), sumStats.getMax(), sumStats.getMean(), sumStats.getStandardDeviation());

		dumpLatencyMeasurements("benchmark12delete", "numSamples=" + numSamples, timings);
		
		// Drop the index
		// Seems like its not possible to do this via the REST API... will need to manually remove
		
		clearDatabase();
	}
}
