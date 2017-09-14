import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

public class MapReduce3 {
	public static void main(String[] args) throws IOException {
        
		///////////////////////////////////////////////////////
		////	How to run on the Command Line
		///		cd to src folder
		///		javac MapReduce3.java
		///		java MapReduce 	file1.txt file2.txt	file3.txt 6
		///
		///
		///		Command Line parameters
		///
		///		The last parameter must be an integer which specifies the
		///		number of threads to run at once in the ThreadPoolExecutor
		///		
		///		Any number of files can be passed before this integer
		///
		///
		//////////////////////////////////////////////////////////
		
    	Map<String, String> input = new HashMap<String, String>();
    	//create a map for all files to be processed
    	for (int i=0;i<args.length-1;i++) {
    		//loop through the command line parameters except the last one
    		String arg = args[i];
            System.out.println("File to be processed\t" + arg);
            String content = new String(Files.readAllBytes(Paths.get(arg)));
            //save the content of each file as a string
            input.put(arg, content);
            //save each file and the content of the file in the input map
        }
    		int ThreadPoolCount = Integer.parseInt(args[args.length-1]);
    		
            
            // APPROACH #3: Distributed MapReduce
            {
                    final Map<String, Map<String, Integer>> output = new HashMap<String, Map<String, Integer>>();
                    
                    // MAP:
                    
                    final List<MappedItem> mappedItems = new LinkedList<MappedItem>();
                    
                    final MapCallback<String, MappedItem> mapCallback = new MapCallback<String, MappedItem>() {
                            @Override
            public synchronized void mapDone(String file, List<MappedItem> results) {
                mappedItems.addAll(results);
            }
                    };
                    
                    List<Thread> mapCluster = new ArrayList<Thread>(input.size());
                    
                    Iterator<Map.Entry<String, String>> inputIter = input.entrySet().iterator();
                    while(inputIter.hasNext()) {
                    	Map.Entry<String, String> entry = inputIter.next();
                    	final String file = entry.getKey();
                    	final String contents = entry.getValue();
                            
                    	Thread t = new Thread(new Runnable() {
                        @Override
                    		public void run() {
                        		map(file, contents, mapCallback);
                        	}
                    	});
                        mapCluster.add(t);
                        t.start();
                    }
                    
                    // wait for mapping phase to be over:
                    for(Thread t : mapCluster) {
                            try {
                                    t.join();
                            } catch(InterruptedException e) {
                                    throw new RuntimeException(e);
                            }
                    }
                    
                    // GROUP:
                    
                    Map<String, List<String>> groupedItems = new HashMap<String, List<String>>();
                    
                    Iterator<MappedItem> mappedIter = mappedItems.iterator();
                    while(mappedIter.hasNext()) {
                            MappedItem item = mappedIter.next();
                            String word = item.getWord();
                            String file = item.getFile();
                            List<String> list = groupedItems.get(word);
                            if (list == null) {
                                    list = new LinkedList<String>();
                                    groupedItems.put(word, list);
                            }
                            list.add(file);
                    }
                    
                    // REDUCE:
                    
                    final ReduceCallback<String, String, Integer> reduceCallback = new ReduceCallback<String, String, Integer>() {
                            @Override
                        	public synchronized void reduceDone(String k, Map<String, Integer> v) {
                            	output.put(k, v);
                            }
                    };
                    
                    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(ThreadPoolCount);
                    Iterator<Map.Entry<String, List<String>>> groupedIter = groupedItems.entrySet().iterator();
                    while(groupedIter.hasNext()) {
                            Map.Entry<String, List<String>> entry = groupedIter.next();
                            final String word = entry.getKey();
                            final List<String> list = entry.getValue();
                            
                            Thread t = new Thread(new Runnable() {
                                    @Override
                                    public void run() {
                                            reduce(word, list, reduceCallback);
                                    }
                            });
                            executor.execute(t);
                            //pass each thread to the thread pool executor
                    }
                    executor.shutdown();
                    while(!executor.isTerminated()){
                    	//Give the program a chance to finish the reduce functions
                    }
                    System.out.println("\nOutput "+output);
                    System.out.println("\nThread Pool is finished?\t"+executor.isTerminated());
                    
            }
    }
    
    public static void map(String file, String contents, List<MappedItem> mappedItems) {
            String[] words = contents.trim().split("\\s+");
            for(String word: words) {
            	String temp = word.substring(0, 1).toUpperCase();
            	//only save the first character of the word as a string and convert to Upper Case
        		if (Character.isLetter(temp.charAt(0))){
        			//only add the word if the first character is a letter
        			mappedItems.add(new MappedItem(temp, file));
        		}
            }
    }
    
    public static void reduce(String word, List<String> list, Map<String, Map<String, Integer>> output) {
            Map<String, Integer> reducedList = new HashMap<String, Integer>();
            for(String file: list) {
                    Integer occurrences = reducedList.get(file);
                    if (occurrences == null) {
                            reducedList.put(file, 1);
                    } else {
                            reducedList.put(file, occurrences.intValue() + 1);
                    }
            }
            output.put(word, reducedList);
    }
    
    public static interface MapCallback<E, V> {
            
            public void mapDone(E key, List<V> values);
    }
    
    public static void map(String file, String contents, MapCallback<String, MappedItem> callback) {
            String[] words = contents.trim().split("\\s+");
            List<MappedItem> results = new ArrayList<MappedItem>(words.length);
            for(String word: words) {
            		String temp = word.substring(0, 1).toUpperCase();
            		//only save the first character of the word as a string and convert to Upper Case
            		if (Character.isLetter(temp.charAt(0))){
            			//only add the word if the first character is a letter
            			results.add(new MappedItem(temp, file));
            		}
            }
            callback.mapDone(file, results);
    }
    
    public static interface ReduceCallback<E, K, V> {
            public void reduceDone(E e, Map<K,V> results);
    }
    
    public static void reduce(String word, List<String> list, ReduceCallback<String, String, Integer> callback) {
            
            Map<String, Integer> reducedList = new HashMap<String, Integer>();
            for(String file: list) {
                    Integer occurrences = reducedList.get(file);
                    if (occurrences == null) {
                            reducedList.put(file, 1);
                    } else {
                            reducedList.put(file, occurrences.intValue() + 1);
                    }
            }
            callback.reduceDone(word, reducedList);
    }
    
    private static class MappedItem { 
            
            private final String word;
            private final String file;
            
            public MappedItem(String word, String file) {
                    this.word = word;
                    this.file = file;
            }

            public String getWord() {
                    return word;
            }

            public String getFile() {
                    return file;
            }
            
            @Override
            public String toString() {
                    return "[\"" + word + "\",\"" + file + "\"]";
            }
    }
}
