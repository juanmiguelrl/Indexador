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



public class SimilarTerms {
		
		
	private SimilarTerms() {
	}


	public static void main(String[] args) {
		String usage = "java es.udc.fi.ri.ri_p1.SimilarTerms -index INDEX_PATH -field FIELD -term TERM" + 
					   " -top N -rep REPRESENTATION\n\n";
		
		String indexPath = null;
		String field = null;
		String term = null;
		int top = -1;
		String rep = null;
		
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
			}
		}
		
		if (indexPath == null || field == null || term == null || top == -1 || rep == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		} else if ( !rep.equals("bin") && !rep.equals("tf") && !rep.equals("tfxidf") ) {
			System.err.println("The -rep parameter must have one of the following values: {bin, tf, tfxidf}");
			System.exit(1);
		}
		
		
		Date start = new Date();
		try {
			dir = FSDirectory.open(Paths.get(indexPath));
			reader = DirectoryReader.open(dir);
			
			output_str += "Top " + Integer.toString(top) + " most similar terms to the term <term, field> = <" + 
						  term + ", " + field + "> represented by " + rep + " (according to cosine similarity)\n\n";
			
			
			
			// Obtenemos el RealVector del term introducido
			Map<Integer, Double> mapInput = getDocFrequencies(reader, field, term, rep);
			RealVector vectorInput = toRealVector(mapInput, reader.maxDoc());
			
			
			
			
			// Map que contiene <similarity, términos>
			// Treemap para guardar la similaridad con una lista de los terminos
			TreeMap<Double, List<String>> mapSimilarity = new TreeMap<>();
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
						output_str +=  topTerminos.next().toString() + ": con similaridad  " + campo.getKey().toString() + "\n";
						i++;
					}
				}
				
				System.out.println(output_str);
			}
					
		
			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

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

}
