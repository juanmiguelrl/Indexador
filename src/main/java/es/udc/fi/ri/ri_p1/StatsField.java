package es.udc.fi.ri.ri_p1;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Date;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;



public class StatsField {


	private StatsField() {
	}
	
	
	public static void main(String[] args) {
		String usage = "java es.udc.fi.ri.ri_p1.StatsField [-index INDEX_PATH] [-field FIELD]\n\n";
		
		IndexReader reader = null;
		String indexPath = "index";
		String field = null;
		Directory dir = null;
		IndexSearcher searcher;
		CollectionStatistics stats;
		
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-field".equals(args[i])) {
				field = args[i + 1];
				i++;
			}
		}


		Date start = new Date();
		try {
			dir = FSDirectory.open(Paths.get(indexPath));
			reader = DirectoryReader.open(dir);
			searcher = new IndexSearcher(reader);
			
			if (field != null) {
				System.out.println("Statistics from field '" + field + "' from index '" + indexPath + "'");
				
				stats = searcher.collectionStatistics(field);
				if (stats != null) {
					System.out.println(stats.toString());
				} else {
					System.out.println("The field '" + field + "' does not exist (has no indexed terms)");
				}
				
			} else {
				System.out.println("Statistics from all fields from index '" + indexPath + "'");
				
				final FieldInfos fieldinfos = FieldInfos.getMergedFieldInfos(reader);
					
				for (final FieldInfo fieldinfo: fieldinfos) {
					
					stats = searcher.collectionStatistics(fieldinfo.name);
					
					//System.out.println("Field = " + fieldinfo.name);
					if (stats != null) {
						System.out.println(stats.toString());
					} 
				}
			}




			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}
    
	}
	
}
