# Information Retrieval Project

### The problem
The scope of this project is the implementation of an index structure based on the document collection "Passage ranking dataset" available on this page: https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020.
This solution allows to handle information retrieval in front of a massive scale of documents, starting
from the design of the data structures needed, implementing a scalable indexing and going towards
query processing.

### Snowball Stemmer Installation 
The snowball stemmer should be installed manually due to the absence of support from Maven using the following command:
<br />
<pre>
mvn install:install-file -Dfile=./resources/libstemmer.jar -Dpackaging=jar -DgroupId=org.tartarus -DartifactId=snowball -Dversion=1.0
</pre> 
The stemmer jar file is included in the resources folder.
<br/>

### How to use our solution
The project contains a ready-to-use jar file that can be used to test our solution.
<br/>
The first parameter can be used to choose if the indexing or the query processing component should be used.


### Indexing component
Indexing can be performed using proper compile flags while running the program.<br/>
If no flags are used indexing is performed by default using the binary format.<br />
Is the index flag is specified, the second flag can determine the type of indexing to be performed.
<pre>
java -jar information-retrieval-project.jar index .DAT
</pre> 
<pre>
java -jar information-retrieval-project.jar index .TXT
</pre> 
The textual format will use the ASCII encoding for debugging.

### Query processing component
The first flag can be set to query in order to start the query processor.
<pre>
java -jar information-retrieval-project.jar query
</pre>
Once launched, the query processing component will require an input query according to this format:
<pre>
Input Format: [AND—OR] term1 ... termN
</pre> 
The top k documents according to BM25 will be given in output.

### Stopwords removal and stemming
Stopwords removal and stemming are used by default.<br />
This setting can be changed using the flags contained in the application.properties file.



