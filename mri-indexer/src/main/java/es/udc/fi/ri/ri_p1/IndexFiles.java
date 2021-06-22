package es.udc.fi.ri.ri_p1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.DateTools;
import org.apache.lucene.document.DateTools.Resolution;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;



public class IndexFiles {
	
	private static boolean update = false;
	private static IndexWriterConfig.OpenMode openmode = OpenMode.CREATE;
	private static boolean onlyFiles = false;
	private static List<String> filesList;
	private static int onlyTopLines = -1;
	private static int onlyBottomLines = -1;
	
	/* Not Indexed, not tokenized, stored. */
	private static final FieldType TYPE_NUMBER = new FieldType();
	private static final FieldType TYPE_TEXTFIELD = new FieldType();
	private static final FieldType TYPE_STRINGFIELD = new FieldType();
	private static final IndexOptions options_NUMBER = IndexOptions.DOCS_AND_FREQS;
	private static final IndexOptions options_TEXTFIELD = IndexOptions.DOCS_AND_FREQS;
	private static final IndexOptions options_STRINGFIELD = IndexOptions.DOCS_AND_FREQS;
	//private static final IndexOptions options = IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS;
	static {
		TYPE_NUMBER.setIndexOptions(options_NUMBER);
		TYPE_NUMBER.setTokenized(false);
		TYPE_NUMBER.setStored(true);
		TYPE_NUMBER.setStoreTermVectors(true);
		TYPE_NUMBER.setStoreTermVectorPositions(false);
		TYPE_NUMBER.freeze();
	}
	
	static {
		TYPE_TEXTFIELD.setIndexOptions(options_TEXTFIELD);
		TYPE_TEXTFIELD.setTokenized(true);
		TYPE_TEXTFIELD.setStored(false);
		TYPE_TEXTFIELD.setStoreTermVectors(true);
		TYPE_TEXTFIELD.setStoreTermVectorPositions(false);
		TYPE_TEXTFIELD.freeze();
	}
	
	static {
		TYPE_STRINGFIELD.setIndexOptions(options_STRINGFIELD);
		TYPE_STRINGFIELD.setTokenized(false);
		TYPE_STRINGFIELD.setStored(false);
		TYPE_STRINGFIELD.setStoreTermVectors(true);
		TYPE_STRINGFIELD.setStoreTermVectorPositions(false);
		TYPE_STRINGFIELD.freeze();
	}
	

	private IndexFiles() {
	}
  

  	public static class WorkerThread implements Runnable {
    	
		private IndexWriter writer;
		private Path file;
		private long lastModified;
	
		public WorkerThread(IndexWriter writer, Path file, long lastModified) {
		  	this.writer = writer;
		  	this.file = file;
		    this.lastModified = lastModified;
		    //System.out.println(lastModified);
	  	}
			
		/**
		 * This is the work that the current thread will do when processed by the pool.
		 * In this case, it will only print some information.
		 */
		@Override
		public void run() {
			//System.out.println(String.format("Thread '%s': indexing file '%s'",Thread.currentThread().getName(), file));
		  	try {
				indexDoc(writer, file, lastModified);
			} catch (IOException ignore) {
				// don't index files that can't be read.
			}
	  	}
    }


	public static void main(String[] args) {
		String usage = "java es.udc.fi.ri.ri_p1.IndexFiles [-index INDEX_PATH] [-openmode OPENMODE] [-update]" + 
					   " [-numThreads NUM_THREADS] [-partialIndexes] [-onlyFiles]\n\n";
		
		String indexPath = "index";
		//boolean create = true;
    	int numthreads = Runtime.getRuntime().availableProcessors();
		String openmode_str = null;
		boolean partialIndexes = false;
		
		for (int i = 0; i < args.length; i++) {
			if ("-index".equals(args[i])) {
				indexPath = args[i + 1];
				i++;
			} else if ("-update".equals(args[i])) {
				//create = false;
				update = true;
				
      		} else if ("-numThreads".equals(args[i])) {
				numthreads = Integer.parseInt(args[i + 1]);
				i++;

			} else if ("-openmode".equals(args[i])) {
				openmode_str = args[i + 1];
				i++;
				
				switch (openmode_str) {
			        case "append":
			            openmode = OpenMode.APPEND;
			            break;
			        case "create":
			            openmode = OpenMode.CREATE;
			            break;
			        case "create_or_append":
			            openmode = OpenMode.CREATE_OR_APPEND;
			            break;
			    }

			} else if ("-partialIndexes".equals(args[i])) {
				partialIndexes = true;
				
			} else if ("-onlyFiles".equals(args[i])) {
				onlyFiles = true;
			}
		}
		
		if (openmode_str != null && !openmode_str.equals("append") && !openmode_str.equals("create") && 
			!openmode_str.equals("create_or_append") ) {
			System.err.println("The -openmode parameter must have one of the following values: {append, create, create_or_append}");
			System.exit(1);
		}
    

    	//inicializar pool de threads
    	final ExecutorService executor = Executors.newFixedThreadPool(numthreads);

    	// Leer el archivo config.properties
    	Properties properties = new Properties();
    	try {
    		properties.load(new FileInputStream(new File("src\\main\\resources\\config.properties")));
    	} catch (FileNotFoundException e) {
    		e.printStackTrace();
    	} catch (IOException e) {
    		e.printStackTrace();
    	}
    	
    	List<String> docsList = Arrays.asList(properties.get("docs").toString().split(" "));
    	List<String> indexList = Arrays.asList(properties.get("partialIndexes").toString().split(" "));
		filesList = Arrays.asList(properties.get("onlyFiles").toString().split(" "));
		try {
			onlyTopLines = Integer.parseInt(properties.get("onlyTopLines").toString());
		} catch (NumberFormatException e) {
			onlyTopLines = -1;
		}catch (NullPointerException e) {
			onlyTopLines = -1;
		}
		try {
			onlyBottomLines = Integer.parseInt(properties.get("onlyBottomLines").toString());
		} catch (NumberFormatException e) {
			onlyBottomLines = -1;
		}catch (NullPointerException e) {
			onlyBottomLines = -1;
		}
    	
    	
		if (properties.get("docs").toString() == null) {
			System.err.println("Debe indicarse el DOCS_PATH en la variable 'docs' del archivo config.properties");
			System.exit(1);
		}

		Path docDir = null;

		Date start = new Date();
		try {
			//System.out.println("Indexing to directory '" + indexPath + "'...");

			//Directory dir = FSDirectory.open(Paths.get(indexPath));
			Directory dir = null;
			Analyzer analyzer = new StandardAnalyzer();
			//IndexWriterConfig iwc = new IndexWriterConfig(analyzer);
			IndexWriterConfig iwc = null;
			// Optional: for better indexing performance, if you
			// are indexing many documents, increase the RAM
			// buffer. But if you do this, increase the max heap
			// size to the JVM (eg add -Xmx512m or -Xmx1g):
			//
			// iwc.setRAMBufferSizeMB(256.0);
			//IndexWriter writer = new IndexWriter(dir, iwc);
			//leer config.properties
			
			iwc = new IndexWriterConfig(analyzer);
			System.out.println("indice final para fusionar = " + indexPath);
			dir = FSDirectory.open(Paths.get(indexPath));
			iwc.setOpenMode(openmode);
			IndexWriter writerFinal = new IndexWriter(dir, iwc);
			
			FSDirectory[] Directories = new FSDirectory[docsList.size()];
			IndexWriter w = null;
			int i = 0;
			String pathToIndex = null;
			//lista de writers
			IndexWriter[] writerList = new IndexWriter[docsList.size()];
			while (i < docsList.size()) { //bucle para cada directorio con archivos a indexar
				if (/**i < indexList.size() &&**/ partialIndexes) {
					pathToIndex = indexList.get(i);
					// Indicamos el OpenMode del IndexWriter
					iwc = new IndexWriterConfig(analyzer);
					System.out.println("pathtoindex = " + pathToIndex);
					Directories[i] = FSDirectory.open(Paths.get(pathToIndex));
					iwc.setOpenMode(openmode);
					writerList[i] = new IndexWriter(Directories[i], iwc);
					
					w = writerList[i];
					
				} else {
					//en este caso si hay menos partialindexes que docs se ejecutaría algunos en el index final
					pathToIndex = indexPath;
					w = writerFinal;
				}


				//obetenemos el directorio para indexar
					docDir = Paths.get(docsList.get(i));
					//System.out.println("docDir = " + docDir);
					if (!Files.isReadable(docDir)) {
						System.out.println("Document directory '" + docDir.toAbsolutePath()
								+ "' does not exist or is not readable, please check the path");
						System.exit(1);
				}
				//comenzamos la indexacion 
				System.out.println("Indexing to directory '" + pathToIndex + "'...");
				indexDocs(w, docDir,executor);
				
				i++;
				//writerPartial.close();
			}
			
			
			/*******************************************************************************************************/
			// parte de cerrar los threads
	    	/*
			 * Close the ThreadPool; no more jobs will be accepted, but all the previously
			 * submitted jobs will be processed.
			 */
			executor.shutdown();

			/* Wait up to 1 hour to finish all the previously submitted jobs */
			try {
				executor.awaitTermination(1, TimeUnit.HOURS);
			} catch (final InterruptedException e) {
				e.printStackTrace();
				System.exit(-2);
			}

			System.out.println("Finished all threads");
			/*******************************************************************************************************/
			
			//writerFinal.addIndexes(writerList);
			
			//cerrar todos los writers
			if (partialIndexes) {
				for (i = 0;i < docsList.size();i++) {
					writerList[i].commit();
					writerList[i].close();
				}
				writerFinal.addIndexes(Directories);
			}
			writerFinal.commit();
			writerFinal.close();
			
			// NOTE: if you want to maximize search performance,
			// you can optionally call forceMerge here. This can be
			// a terribly costly operation, so generally it's only
			// worth it when your index is relatively static (ie
			// you're done adding documents to it):
			//
			// writer.forceMerge(1);
			
			
			//writer.close();


			Date end = new Date();
			System.out.println(end.getTime() - start.getTime() + " total milliseconds");

		} catch (IOException e) {
			System.out.println(" caught a " + e.getClass() + "\n with message: " + e.getMessage());
		}
    
	}
	
	
	private static String getExtension(Path path) {
		String fileName = path.toString();
		String extension = "";
		
		int i = fileName.lastIndexOf('.');
		if (i > 0) {
		    extension = fileName.substring(i);
		}
		
		return extension;
	}


	/**
	 * Indexes the given file using the given writer, or if a directory is given,
	 * recurses over files and directories found under the given directory.
	 * 
	 * NOTE: This method indexes one document per input file. This is slow. For good
	 * throughput, put multiple documents into your input file(s). An example of
	 * this is in the benchmark module, which can create "line doc" files, one
	 * document per line, using the <a href=
	 * "../../../../../contrib-benchmark/org/apache/lucene/benchmark/byTask/tasks/WriteLineDocTask.html"
	 * >WriteLineDocTask</a>.
	 * 
	 * @param writer Writer to the index where the given file/dir info will be
	 *               stored
	 * @param path   The file to index, or the directory to recurse into to find
	 *               files to index
	 * @throws IOException If there is a low-level I/O error
	 */
	static void indexDocs(final IndexWriter writer, Path path, ExecutorService executor) throws IOException {
		if (Files.isDirectory(path)) {
			Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
				@Override
				public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
					try {
						String ext = getExtension(file);
						if (!onlyFiles || filesList.contains(ext)) {
							//indexDoc(writer, file, attrs.lastModifiedTime().toMillis());
							final Runnable worker = new WorkerThread(writer, file, Files.getLastModifiedTime(path).toMillis());
							executor.execute(worker);
						}
					} catch (IOException ignore) {
						// don't index files that can't be read.
					}
					return FileVisitResult.CONTINUE;
				}
			});
		} else {
			String ext = getExtension(path);
			if (!onlyFiles || filesList.contains(ext)) {
				//indexDoc(writer, path, Files.getLastModifiedTime(path).toMillis());
		      	final Runnable worker = new WorkerThread(writer, path, Files.getLastModifiedTime(path).toMillis());
		      	executor.execute(worker);
			}
		}
	}


	/** Indexes a single document */
	static void indexDoc(IndexWriter writer, Path file, long lastModified) throws IOException {
		try (InputStream stream = Files.newInputStream(file)) {
			// make a new, empty document
			Document doc = new Document();
			
			//extraer atributos del archivo
            BasicFileAttributes attributes = Files.readAttributes(file, BasicFileAttributes.class);

			// Add the path of the file as a field named "path". Use a
			// field that is indexed (i.e. searchable), but don't tokenize
			// the field into separate words and don't index term frequency
			// or positional information:
			//Field pathField = new StringField("path", file.toString(), Field.Store.YES);
			//doc.add(pathField);
			doc.add(new Field("path", file.toString(), TYPE_STRINGFIELD));

			// Add the last modified date of the file a field named "modified".
			// Use a LongPoint that is indexed (i.e. efficiently filterable with
			// PointRangeQuery). This indexes to milli-second resolution, which
			// is often too fine. You could instead create a number based on
			// year/month/day/hour/minutes/seconds, down the resolution you require.
			// For example the long value 2011021714 would mean
			// February 17, 2011, 2-3 PM.
			doc.add(new LongPoint("modified", lastModified));
			doc.add(new Field("modifiedStr", Long.toString(lastModified), TYPE_NUMBER));
			//doc.add(new StringField("modifiedStringField", Long.toString(lastModified), Field.Store.YES));

			// Add the contents of the file to a field named "contents". Specify a Reader,
			// so that the text of the file is tokenized and indexed, but not stored.
			// Note that FileReader expects the file to be in UTF-8 encoding.
			// If that's not the case searching for special characters will fail.
			
			if ((onlyTopLines == -1) && (onlyBottomLines == -1)) {
				//doc.add(new TextField("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8))));
				doc.add(new Field("contents", new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8)), TYPE_TEXTFIELD));

			} else {
				List<String> content = new ArrayList<String>();
				String finalContent = "";
				BufferedReader buffer = new BufferedReader(new InputStreamReader(stream, StandardCharsets.UTF_8));
				String line = "";
				line = buffer.readLine();
				while (line != null) {
					content.add(line);
					line = buffer.readLine();
				};
				
				for (int i = 0; (i < content.size()); i++) {
					if ( (i < onlyTopLines) || (i > (content.size()-1 - onlyBottomLines)) ) {
						finalContent += content.get(i) + "\n";
					}
				}

				StringReader reader = new StringReader(finalContent);
				//doc.add(new TextField("contents", reader));
				doc.add(new Field("contents", reader, TYPE_TEXTFIELD));
			}
			
			
			//añadir hostname
			//doc.add(new StringField("hostname", InetAddress.getLocalHost().getHostName(), Field.Store.YES));
			doc.add(new Field("hostname", InetAddress.getLocalHost().getHostName(), TYPE_STRINGFIELD));
			
			//añadir thread name
			//doc.add(new StringField("thread", Thread.currentThread().getName(), Field.Store.YES));
			doc.add(new Field("thread", Thread.currentThread().getName(), TYPE_STRINGFIELD));
			
			//añadir tamaño del archivo
			float fileSizeInKB = Files.size(file) / 1024;
			doc.add(new FloatPoint("sizeKb", fileSizeInKB));
			doc.add(new Field("sizeKbStr", Float.toString(fileSizeInKB), TYPE_NUMBER));
			
			//añadir fecha de creacion
			FileTime creationTime = attributes.creationTime();
			SimpleDateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
			String creationTimeString = df.format(creationTime.toMillis());
			//doc.add(new StringField("creationTime", creationTimeString, Field.Store.YES));
			doc.add(new Field("creationTime", creationTimeString, TYPE_STRINGFIELD));
			
			//añadir fecha de ultimo acceso
			FileTime lastAccessTime = attributes.lastAccessTime();
			String lastAccessTimeString = df.format(lastAccessTime.toMillis());
			//doc.add(new StringField("lastAccessTime", lastAccessTimeString, Field.Store.YES));	
			doc.add(new Field("lastAccessTime", lastAccessTimeString, TYPE_STRINGFIELD));
			
			//añadir fecha de ultima modificacion
			FileTime lastModifiedTime = attributes.lastModifiedTime();
			String lastModifiedTimeString = df.format(lastModifiedTime.toMillis());
			//doc.add(new StringField("lastModifiedTime", lastModifiedTimeString, Field.Store.YES));
			doc.add(new Field("lastModifiedTime", lastModifiedTimeString, TYPE_STRINGFIELD));
			
			
			//pasar creationTime a formato lucene
			Date creationTimeDate = new Date(creationTime.toMillis());
			String creationTimeLucene = DateTools.dateToString(creationTimeDate, Resolution.SECOND);
			//doc.add(new StringField("creationTimeLucene", creationTimeLucene, Field.Store.YES));
			doc.add(new Field("creationTimeLucene", creationTimeLucene, TYPE_STRINGFIELD));
			
			//pasar fecha de ultimo acceso a formato Lucene
			Date lastAccessTimeDate = new Date(lastAccessTime.toMillis());
			String lastAccessTimeLucene = DateTools.dateToString(lastAccessTimeDate,Resolution.SECOND);
			//doc.add(new StringField("lastAccessTimeLucene", lastAccessTimeLucene, Field.Store.YES));
			doc.add(new Field("lastAccessTimeLucene", lastAccessTimeLucene, TYPE_STRINGFIELD));
			
			//pasar fecha de ultima modificacion a formato Lucene
			Date lastModifiedTimeDate = new Date(lastModifiedTime.toMillis());
			String lastModifiedTimeLucene = DateTools.dateToString(lastModifiedTimeDate,Resolution.SECOND);
			//doc.add(new StringField("lastModifiedTimeLucene", lastModifiedTimeLucene, Field.Store.YES));
			doc.add(new Field("lastModifiedTimeLucene", lastModifiedTimeLucene, TYPE_STRINGFIELD));
			

			// Comprobamos si el index existe
			//Directory directorio = FSDirectory.open(file);
			//System.out.println("es el archivo  " + file);
			//System.out.println(writer.getDirectory());
			boolean index_exists = DirectoryReader.indexExists(writer.getDirectory());
			
			if (openmode == OpenMode.CREATE) {
				System.out.println("adding " + file);
				writer.addDocument(doc);
				
			} else if (openmode == OpenMode.APPEND) {
				if (update) {
					System.out.println("updating " + file);
					writer.updateDocument(new Term("path", file.toString()), doc);
				} else {
					System.out.println("adding " + file);
					writer.addDocument(doc);
				}
				
			} else if (openmode == OpenMode.CREATE_OR_APPEND && !index_exists) {
				System.out.println("adding " + file);
				writer.addDocument(doc);
				
			} else if (openmode == OpenMode.CREATE_OR_APPEND && index_exists) {
				if (update) {
					System.out.println("updating " + file);
					writer.updateDocument(new Term("path", file.toString()), doc);
				} else {
					System.out.println("adding " + file);
					writer.addDocument(doc);
				}
			}
		}
	}
}
