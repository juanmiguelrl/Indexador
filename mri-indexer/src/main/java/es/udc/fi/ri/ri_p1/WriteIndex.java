package es.udc.fi.ri.ri_p1;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Paths;
import java.util.Date;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiTerms;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;



public class WriteIndex {


	private WriteIndex() {
	}
	
	
	public static void main(String[] args) {
		String usage = "java es.udc.fi.ri.ri_p1.WriteIndex -index INDEX_PATH -outputfile OUTPUT_PATH\n\n";
		
		IndexReader reader = null;
		String indexPath = null;
		String outputPath = null;
		Directory dir = null;
		
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-outputfile".equals(args[i])) {
				outputPath = args[i + 1];
				i++;
			}
		}

		if (indexPath == null || outputPath == null) {
			System.err.println("Usage: " + usage);
			System.exit(1);
		}
		
	
		Date start = new Date();
		try {
			dir = FSDirectory.open(Paths.get(indexPath));
			reader = DirectoryReader.open(dir);

			Writer fileWriter = new FileWriter(outputPath, false);
			
			final FieldInfos fieldinfos = FieldInfos.getMergedFieldInfos(reader);
			
			
			for (final FieldInfo fieldinfo: fieldinfos) {

				fileWriter.write("Terms of field '" + fieldinfo.name + "':\n");

				final Terms terms = MultiTerms.getTerms(reader, fieldinfo.name);
				if (terms != null) {
					final TermsEnum termsEnum = terms.iterator();
					while (termsEnum.next() != null) {
						fileWriter.write(termsEnum.term().utf8ToString() + "\n");
					}	
				}
				fileWriter.write("\n\n");
			}
			fileWriter.close();
			
			
			System.out.println("Created file " + outputPath);
			
			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}
		
	}

}
