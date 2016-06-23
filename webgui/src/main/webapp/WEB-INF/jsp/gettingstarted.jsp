<%-- 
    Document   : welcome
    Created on : Apr 8, 2014, 2:15:56 PM
    Author     : turnguard
--%>

<%@page contentType="text/html" pageEncoding="UTF-8"%>
<div>
<p>
SemaGrow is available as a .deb distribution. To install on Debian based system you need to execute the following on a terminal:
</p>
<p>
<code>
# add debian sources and key<br/>
echo 'deb http://semagrow.semantic-web.at/deb/ lucid free' > /etc/apt/sources.list.d/semagrow.list<br/>
wget -q http://semagrow.semantic-web.at/deb/packages.semagrow.key -O- | apt-key add -
</code>
</p>
<code>
# install SemaGrow and dependencies<br/>
apt-get update<br/>
apt-get install openjdk-7-jdk semagrow
</code>

<p>
To start the SemaGrow endpoint, issue:
</p>
<code>
service semagrow start<br/>
service semagrow status
</code>

<p>
At this point, the SemaGrow stack should be up and running and the output of the “service semagrow status” command should be “[ ok ] SemaGrow Stack is running with pid ****.”.
</p>
<p>
To access SemaGrow Stack endpoint through a browser, enter the following url: http://&lt;ip&gt;:8080/SemaGrow, where &lt;ip&gt; is the IP of your computer. 
By default, SemaGrow uses the 8080 port
</p>
<p>
At http://143.233.226.43:8080/SemaGrow you can find a publicly available example deployment of the SemaGrow stack.
</p>
<p>
The logs of the SemaGrow stack are found in your computer in the path: /var/log/semagrow/
</p>
<p>
The information that the SemaGrow stack needs regarding the available datasets, in the form of metadata, is found locally in your computer in the turtle file metadata.ttl. This file is located at the path:
</p>
<code>
/etc/default/semagrow/metadata.ttl
</code>
<p>
This file contains all the necessary information regarding the available datasets that the SemaGrow stack can access in order to get the data the user requests through SemaGrow using SPARQL queries.<br/>
The metadata.ttl file can be modified with a text editor or, ideally, using the Eleon metadata editor. You can find detailed information on the usage of the Eleon metadata editor at the Eleon User Guide.
</p>
<p>
The usage of the SemaGrow stack is performed through a browser, by accessing http://&lt;ip&gt;:8080/SemaGrow
Clicking on the “Sparql” tab presents the SPARQL query environment.<br/>
<a href="resources/images/screenshot_1.png" target="_blank"><img src="resources/images/screenshot_1.png" width="800"/></a>
<br/>
</p>
<p>
By submitting SPARQL queries, one can get relevant results from the datasets whose metadata information is provided through the metadata.ttl file. <br/>
In our example, the metadata.ttl file contains metadata regarding the AGRIS dataset, which is an international information system for the agricultural science and technology.
</p>
<p>
You can see a simple usage of the SemaGrow stack by submitting the SPARQL query already filled out in the SPARQL query box:
</p>
<code>
SELECT * WHERE {
  ?s ?p ?o
} LIMIT 20
</code>
<p>
Simply press the SPARQL button below the box (by pressing the SPARQL button, SemaGrow executes the query written inside the query box). <br/>
This will return and show on screen the first 20 results from AGRIS. <br/>
<a href="resources/images/screenshot_2.png" target="_blank"><img src="resources/images/screenshot_2.png" width="800"/></a><br/>
As can be seen in the screenshot, the query was successfully executed and the results are shown on the right side of the page, in JSON format.
</p>

<p>
The great benefit from using the SemaGrow stack is that by altering the metadata file, especially by adding metadata of new datasets semantically relevant to the ones already used, <br/>
SemaGrow can “understand” the relevance between the datasets and thus provide an even greater and enriched result set, even by submitting the same query as before the metadata change.
</p>

<p>
EXAMPLE
<br/>
Let's say one wants to get the agricultural images published between the years 2006 and 2008. <br/>
The SPARQL query that can provide this result set is (the metadata file contains information only for the AGRIS dataset and can be found at *):
</p>

<code>
SELECT ?s ?a WHERE {
  ?s <http://purl.org/dc/terms/issued> ?a.
   ?s <http://purl.org/dc/terms/type> "Image".
   FILTER(xsd:integer(?a) > 2005).
   FILTER(xsd:integer(?a) < 2009).
}
</code>
<p>
The results showing the title of the publications (published between 2006 and 2008) will be shown on the right side of the page.<br/>
<a href="resources/images/screenshot_3.png" target="_blank"><img src="resources/images/screenshot_3.png" width="800"/></a><br/>
</p>

<p>
In order to receive the total number of those publications, we can execute the following query:
</p>
<code>
SELECT (COUNT(?s) AS ?no) WHERE {
   ?s <http://purl.org/dc/terms/issued> ?a.
   ?s <http://purl.org/dc/terms/type> "Image".
   FILTER(xsd:integer(?a) > 2005).
   FILTER(xsd:integer(?a) < 2009).
}
</code>
<br/>
<a href="resources/images/screenshot_4.png" target="_blank"><img src="resources/images/screenshot_4.png" width="800"/></a><br/>
<p>
As mentioned above, the metadata file can be enriched with new datasets that the user wants to be available to the SemaGrow stack for accessing and querying. So, using the Eleon metadata editor, we add a new dataset that contains information specifically for the year of publication of the publications contained in AGRIS. The difference is that the new dataset contains a “cleaner” version of the year of publication, than that of the AGRIS dataset, thus being able to match more publications to a specific year, and in that way acquiring, by executing the same query as before, a larger number of results (superset of the previous result set). After the addition of the new dataset, the articles / publications are exactly the same as before, only now we got a dataset with “cleaner” dates matched to those publications. We can confirm that, by executing the same query as before:
</p>
<code>
SELECT (COUNT(?s) AS ?no) WHERE {
   ?s <http://purl.org/dc/terms/issued> ?a.
   ?s <http://purl.org/dc/terms/type> "Image".
   FILTER(xsd:integer(?a) > 2005).
   FILTER(xsd:integer(?a) < 2009).
}
</code>
<p>
After having replaced the old metadata.ttl file with the one containing new information for the year_of_publication dataset (the metadata-2.ttl file that accompanies this document), the number of results is greater than the one before.
</p>
<br/>
<a href="resources/images/screenshot_5.png" target="_blank"><img src="resources/images/screenshot_5.png" width="800"/></a><br/>
<p>
This is due to the fact that SemaGrow joined the two related datasets and, since the new dataset can match more triples that satisfy the 
</p>
<code>
FILTER(xsd:integer(?a) > 2005). FILTER(xsd:integer(?a) < 2009).
</code>
<p>
triple patterns to publications, it gives back as a result the union of the two result sets, that now contains the results of the first dataset as well as additional results from the second dataset, <br/>
that were not contained in the first one, because of ambiguous statement of the publication year, as in “10/11/07”.
</p>
</div>    