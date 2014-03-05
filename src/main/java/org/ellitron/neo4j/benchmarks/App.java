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

/**
 * App demonstrates the use of {@link HttpClientBenchmarks}
 * 
 * @author Jonathan Ellithorpe
 *
 */
public class App 
{
	public static void main(String[] args) {
		System.out.println("Welcome to Neo4j Benchmarks!");
		
		HttpClientBenchmarks benchmarks = new HttpClientBenchmarks("http://192.168.1.101:7474/db/data/");
		
		benchmarks.clearDatabase();
		benchmarks.warmUpDatabase((int)1e5);
		benchmarks.run01((int)1e4);	// Read nodes
		benchmarks.run02((int)1e4);	// Read edges
		benchmarks.run05((int)1e4);	// Create nodes
		benchmarks.run06((int)1e4);	// Create edges
		benchmarks.run07((int)1e4);	// Update node properties
		benchmarks.run08((int)1e4);	// Update edge properties
		benchmarks.run09((int)1e4);	// Delete nodes
		benchmarks.run10((int)1e4);	// Delete edges
		benchmarks.run11((int)1e4); // CRUD without indices (cypher based queries)
		benchmarks.run12((int)1e4); // CRUD using indices (cypher based queries)
	}
}
