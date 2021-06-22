package es.udc.fi.ri.ri_p1;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;



public class BestTerms {
	
	//private static final Set<String> terms = new HashSet<>();


	private BestTerms() {
	}
	
	
	public static void main(String[] args) {
		String usage = "java es.udc.fi.ri.ri_p1.BestTerms -index INDEX_PATH -docID DOC_ID -field FIELD" + 
					   " -top N -order ORDER [-outpufile OUTPUT_PATH]\n\n";
				
		String indexPath = null;
		int docID = -1;
		String field = null;
		int top = -1;
		String order = null;
		String outputPath = null;
		
		Directory dir = null;
		IndexReader reader = null;
		String output_str = "";
		
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-docID".equals(args[i])) {
				docID = Integer.parseInt(args[i + 1]);
				i++;
			} else if ("-field".equals(args[i])) {
				field = args[i + 1];
				i++;
			} else if ("-top".equals(args[i])) {
				top = Integer.parseInt(args[i + 1]);
				i++;
			} else if ("-order".equals(args[i])) {
				order = args[i + 1];
				i++;
			} else if ("-outputfile".equals(args[i])) {
				outputPath = args[i + 1];
				i++;
			}
		}
		
		if (indexPath == null || docID == -1 || field == null || top == -1 || order == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		} else if ( !order.equals("tf") && !order.equals("df") && !order.equals("tfxidf") ) {
			System.err.println("The -order parameter must have one of the following values: {tf, df, tfxidf}");
			System.exit(1);
		}
	
	
		Date start = new Date();
		try {
			dir = FSDirectory.open(Paths.get(indexPath));
			reader = DirectoryReader.open(dir);
			

			output_str += "Top " + top + " terms ordered by '" + order + "'\n\n";
			

			TreeMap<Double, List<Termino>> mapa = getTerminos(reader, docID, field, order);
			Iterator<Termino> topTerminos = null;
			Entry<Double, List<Termino>> campo;
			
			int i = 0;
			while (i < top) {
				campo = mapa.pollLastEntry();
				if (campo == null) {
					break;
				}
				topTerminos  = campo.getValue().iterator();
				while((i < top) && topTerminos.hasNext()) {
					output_str +=  topTerminos.next().toString() + "\n";
					i++;
				}
			}
		
			
			// Salida por pantalla o en archivo
			if (outputPath == null) {
				System.out.println(output_str);
				
			} else {
				Writer fileWriter = new FileWriter(outputPath, false);
				fileWriter.write(output_str);
				fileWriter.close();
			}
			
						
			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}
	}

	
	private static TreeMap<Double, List<Termino>> getTerminos(IndexReader reader, int docId, String field, String order) throws IOException {
		Terms vector = reader.getTermVector(docId, field);
		// IndexReader.getTermVector(int docID, String field):
		// Retrieve term vector for this document and field, or null if term
		// vectors were not indexed.
		// The returned Fields instance acts like a single-document inverted
		// index (the docID will be 0).

		// Por esta razon al iterar sobre los terminos la totalTermFreq que es
		// la frecuencia
		// de un termino en la coleccion, en este caso es la frecuencia del
		// termino en docID,
		// es decir, el tf del termino en el documento docID

		TermsEnum termsEnum = null;
		termsEnum = vector.iterator();
		TreeMap<Double, List<Termino>> frequencies = new TreeMap<>();
		BytesRef text = null;
		
		while ((text = termsEnum.next()) != null) {

			String name = text.utf8ToString();
			Double tf = (double) termsEnum.totalTermFreq();
			//Double df = (double) termsEnum.docFreq();
			Term termn = new Term(field, name);
			Double df = (double) reader.docFreq(termn);
			
			Double tfxidf = tf * Math.log10((double) reader.maxDoc()/ df);

			
			Termino t = new Termino(name, tf, df, tfxidf);
			Double key = null;
			
			if (order.equals("tf")) {
				key = tf;
			} else if (order.equals("df")) {
				key = df;
			} else if (order.equals("tfxidf")) {
				key = tfxidf;
			}
			
			if (frequencies.containsKey(key)) {
				frequencies.get(key).add(t);
				
			} else {
				List<Termino> term_list = new ArrayList<>();
				term_list.add(t);
				frequencies.put(key, term_list);
			}

			//terms.add(term);
		}
		return frequencies;
	}


}
