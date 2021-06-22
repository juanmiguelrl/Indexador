package es.udc.fi.ri.ri_p1;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;



public class TermsClusters {
		
		
	private TermsClusters() {
	}


	public static void main(String[] args) {
		String usage = "java es.udc.fi.ri.ri_p1.TermsClusters -index INDEX_PATH -field FIELD -term TERM" + 
					   " -top N -rep REPRESENTATION -k NUM_CLUSTERS\n\n";
		
		String indexPath = null;
		String field = null;
		String term = null;
		int top = -1;
		String rep = null;
		int numClusters = -1;
		
		Directory dir = null;
		IndexReader reader = null;
		String output_str = "";
		
		Set<String> terms = new HashSet<>();
		
		
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-field".equals(args[i])) {
				field = args[i + 1];
				i++;
			} else if ("-term".equals(args[i])) {
				term = args[i + 1];
				i++;
			} else if ("-top".equals(args[i])) {
				top = Integer.parseInt(args[i + 1]);
				i++;
			} else if ("-rep".equals(args[i])) {
				rep = args[i + 1];
				i++;
			} else if ("-k".equals(args[i])) {
				numClusters = Integer.parseInt(args[i + 1]);
				i++;
			}
		}
		
		if (indexPath == null || field == null || term == null || top == -1 || rep == null || numClusters == -1) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		} else if ( !rep.equals("bin") && !rep.equals("tf") && !rep.equals("tfxidf") ) {
			System.err.println("The -rep parameter must have one of the following values: {bin, tf, tfxidf}");
			System.exit(1);
		}
		
		
		// Lista que servirá de input para el algoritmo k-means
		List<Par> pares = new ArrayList<>();
		
		
		Date start = new Date();
		try {
			dir = FSDirectory.open(Paths.get(indexPath));
			reader = DirectoryReader.open(dir);
			
			output_str += "Top " + Integer.toString(top) + " most similar terms to the term <term, field> = <" + 
						  term + ", " + field + "> represented by " + rep + " (according to cosine similarity)\n\n";
			
			
			// Obtenemos el RealVector del term introducido
			Map<Integer, Double> mapInput = getDocFrequencies(reader, field, term, rep);
			RealVector vectorInput = toRealVector(mapInput, reader.maxDoc());
			
			
			
			TreeMap<Double, List<String>> mapSimilarity = new TreeMap<>(); //treemap para guardar la similaridad con una lista de los terminos
			//Map<Double, List<String>> frequencies = new HashMap<>();
			
			final Terms terms2 = MultiTerms.getTerms(reader, field);
			if (terms != null) {
				final TermsEnum termsEnum = terms2.iterator(); //para iterar por los terminos
		
				while (termsEnum.next() != null) {
		
					String termString = termsEnum.term().utf8ToString(); // Nombre del termino
					
					
					//obtener la tf de un termino para cada documento
					Map<Integer, Double> frecuencia = getDocFrequencies(reader, field, termString, rep);
					//pasarla a un real vector
					RealVector ff = toRealVector(frecuencia, reader.maxDoc());
					//obtener la similaridad
					double similaridad = getCosineSimilarity(ff,vectorInput);	

					/**********************************************************************************/
					//TreeMap<Double, List<Termino>> mapSimilarity = getTerminos(reader, docID, field, order);
					
					
					if (mapSimilarity.containsKey(similaridad)) {
						mapSimilarity.get(similaridad).add(termString);
						
					} else {
						List<String> term_list = new ArrayList<>();
						term_list.add(termString);
						mapSimilarity.put(similaridad, term_list);
					}
				}

				Iterator<String> topTerminos = null;
				Entry<Double, List<String>> campo;
					
				
				
				int i = 0;
				while (i < top) {
					campo = mapSimilarity.pollLastEntry();
					if (campo == null) {
						break;
					}
					topTerminos  = campo.getValue().iterator();
					
					while((i < top) && topTerminos.hasNext()) {
						// Añadir término a pares
						String termino = topTerminos.next().toString();
						Double sim = campo.getKey();
						pares.add(new Par(termino, sim));
						
						// Imprimir término por pantalla
						output_str +=  "Term: " + termino + "\tSimilarity: " + sim.toString() + "\n";
						i++;
					}
				}
				
				System.out.print(output_str);
				
			}
			System.out.print("\n\n\n");
			
			
			// Utilizar algoritmo k-means para producir k clusters
			kmeans(numClusters, pares);
			
			/*
			System.out.println("---------------------------------------------------------------------");
			System.out.println("Lista 'pares':");
			for (int i = 0; i < pares.size(); i++) {
				System.out.println("Term: " + pares.get(i).getTerm() + "\tSimilarity: " + pares.get(i).getSim().toString());
			}
			System.out.println();
			System.out.println("top = " + top + "\t pares.size() = " + pares.size());
			System.out.println("---------------------------------------------------------------------");
			*/
			
			
			Date end = new Date();
			System.out.println("\n" + (end.getTime() - start.getTime()) + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}
	}
	
	
	private static Map<Integer, Double> getDocFrequencies(IndexReader reader, String field, String term, String rep) throws IOException {
		
		// Map que contiene <docID, tf>
		Map<Integer, Double> frequencies = new HashMap<>();
		
		PostingsEnum posting = MultiTerms.getTermPostingsEnum(reader, field, new BytesRef(term));

		// If the term does not appear in any document, the posting object may be null
		if (posting != null) {
			int docid;
			// Each time you call posting.nextDoc(), it moves the cursor of the posting list to the next position
			// and returns the docid of the current entry (document). Note that this is an internal Lucene docid.
			// It returns PostingsEnum.NO_MORE_DOCS if you have reached the end of the posting list.
			
			while ((docid = posting.nextDoc()) != PostingsEnum.NO_MORE_DOCS) {
				int freq = posting.freq(); 		// Get the frequency of the term in the current document
				double aux = -1;
				
				// Decidimos como representar los términos según 'rep'
				if (rep.equals("bin")) {
					aux = (freq > 0) ? 1 : 0;
					
				} else if (rep.equals("tf")) {
					aux = freq;
					
				} else if (rep.equals("tfxidf")) {
					Term termn = new Term(field, term);
					Double df = (double) reader.docFreq(termn);
					Double tfxidf = freq * Math.log10((double) reader.maxDoc() / df);
					aux = tfxidf;
				}
				
				
				frequencies.put(docid, aux);
			}
		}
		
		return frequencies;
	}
	
	
	static RealVector toRealVector(Map<Integer, Double> map, Integer docNumber) {
		RealVector vector = new ArrayRealVector(docNumber);
		int i = 0;
		int j = 0;
		for (j = 0; j < docNumber; j++) {
			double value = map.containsKey(j) ? map.get(j) : 0;
			vector.setEntry(i++, value);
		}
		return (RealVector) vector;
	}
	
	static double getCosineSimilarity(RealVector v1, RealVector v2) {
		return (v1.dotProduct(v2)) / (v1.getNorm() * v2.getNorm());
	}

	
	/*************************************************************************************************************************/

	
	/*
	Implementation of k-means algorithm for k clusters
	Author: Xes
	*/
	private static void kmeans(int k, List<Par> pares) {
		int n = pares.size();
		List<Double> centroids = new ArrayList<>();		// centroids.size() = k
		List<List<Par>> clusters = new ArrayList<>();	// clusters.size() = k
		boolean changes;
		int it = 0;
		final int MAX_ITS = 100000;
		
		boolean repeated;
		int count;
		final int MAX_ATTEMPTS = 1000;
		
		// Inicializamos clusters
		for (int i = 0; i < k; i++) {
			List<Par> cluster = new ArrayList<>();
			clusters.add(cluster);
		}

		
		System.out.println("Calculating k-means...");
		
		// Elegimos los centroides
		// Elegimos muestras aleatoriamente, comprobando que no hay valores repetidos
		for (int i = 0; i < k; i++) {
			int x;
			double sim;
			
			System.out.println("---------------------------------");
			count = 0;
			do {
				count++;
				repeated = false;
			
				x = getRandomNumber(0, n);
				sim = pares.get(x).getSim();
				
				System.out.println("Centroid " + i + " = " + sim);
				
				for (double c : centroids) {
					if (sim == c) {
						repeated = true;
					}
				}
				
			} while (repeated && count < MAX_ATTEMPTS);
			
			if (repeated) {
				System.err.println();
				System.err.println("kmeans error: it was not possible to get enough different values to initialize the centroids");
				System.exit(1);
			} else {
				centroids.add(sim);
			}
		}
		System.out.println("---------------------------------");
		
		
		
		do {
			it++;
			if (it < 100 || it % 1000 == 0) {
				System.out.println("Starting iteration " + it);
			}
			
			if (it > MAX_ITS) {
				System.err.println("kmeans error: reached " + MAX_ITS + " iterations without convergence");
				break;
			}
			
			
			changes = false;
			
			// Inicializamos clusters
			for (int i = 0; i < k; i++) {
				List<Par> cluster = clusters.get(i);
				cluster = new ArrayList<>();
				clusters.set(i, cluster);
			}
			
			// Añadimos cada término al centroide del que esté más próximo
			for (Par par : pares) {
				double term_sim = par.getSim();
				
				double distance;
				double min_distance = Double.MAX_VALUE;
				int closer_centroid = -1;
				
			    for (int i = 0; i < k; i++) {
			    	Double centroid_sim = centroids.get(i);
			    	
			    	distance = Math.abs(term_sim - centroid_sim);
			    	if (distance < min_distance) {
			    		min_distance = distance;
			    		closer_centroid = i;
			    	}
			    }
			    List<Par> cluster = clusters.get(closer_centroid);
			    cluster.add(par);
			    clusters.set(closer_centroid, cluster);
			}
			
			// Calculamos la media de cada cluster, actualizamos valores centroides
			for (int i = 0; i < k; i++) {
				List<Par> cluster = clusters.get(i);
				
				double sum = 0;
				for (Par par : cluster) {
					sum += par.getSim();
				}
				double mean = sum / cluster.size();
				
				if (centroids.get(i) != mean) {
					centroids.set(i, mean);
					changes = true;
				}
			}
			
		} while (changes == true);
		System.out.println("Finished k-means (" + it + " iterations)");
		System.out.print("\n\n\n");
		
		
		// Imprimimos los clusters por pantalla
		System.out.println("Terms distributed in " + k + " clusters" + "\n");
		for (int i = 0; i < k; i++) {
			List<Par> cluster = clusters.get(i);
			System.out.println("Cluster " + i + ", centroid = " + centroids.get(i) + ", elements = " + cluster.size() + ":");
			
			for (Par par : cluster) {
				System.out.println("Term: " + par.getTerm() + "\tSimilarity: " + par.getSim());
			}
			System.out.println();
		}
	}
	
	private static int getRandomNumber(int min, int max) {
	    return (int) ((Math.random() * (max - min)) + min);
	}

}
