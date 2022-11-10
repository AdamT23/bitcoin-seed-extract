//Load CSV file created from BQ Public Data Set
//bigquery-public-data.crypto_bitcoin.transactions

//QUERY_TEMPLATE = """
//SELECT
//    block_timestamp AS timestamp,
//    ARRAY_TO_STRING(inputs.addresses, ",") AS input_key,
//    ARRAY_TO_STRING(outputs.addresses, ",") AS output_key,
//    outputs.value as satoshis
//FROM `bigquery-public-data.crypto_bitcoin.transactions`
//    JOIN UNNEST (inputs) AS inputs
//    JOIN UNNEST (outputs) AS outputs
//WHERE ARRAY_TO_STRING(inputs.addresses, ",") IN UNNEST({0})
//    AND outputs.value  >= {1}
//    AND inputs.addresses IS NOT NULL
//    AND outputs.addresses IS NOT NULL
//GROUP BY timestamp, input_key, output_key, satoshis
//"""
//CSV File to read into neo4j
//Directory: /Users/adamturner/Documents/MPICT/Research/9_PhD/data/devel
// trace_transactions.csv
//SAMPLE
//timestamp,input_key,output_key,satoshis
//2017-08-03 03:39:15+00:00,13AM4VW2dhxYgXeQepoHkHSQuy6NgaEb94,1H68h8qsVkMUgY8khcdFpbHV22cCnC74dk,956285137
//2017-08-03 04:28:20+00:00,13AM4VW2dhxYgXeQepoHkHSQuy6NgaEb94,1M1CfXLynR6vqbjwTqSiiLRVDQZEXHHJbb,1005800019
//2017-08-03 03:39:15+00:00,13AM4VW2dhxYgXeQepoHkHSQuy6NgaEb94,1ARirZgU4q61sSjVK2iB8BEYC5w2B8ZnE9,10287428
//2017-08-03 06:45:42+00:00,1ARirZgU4q61sSjVK2iB8BEYC5w2B8ZnE9,1CCJCTiotg75x826rmWJzSFKtrnBaqjQWu,12980376
//2017-08-03 08:05:33+00:00,1M1CfXLynR6vqbjwTqSiiLRVDQZEXHHJbb,16LtUf54vnwvQkcdDTFYtGAeYC5G31yQMG,183268450

//file:///transactions_ph_1_2.csv

USING PERIODIC COMMIT 500
LOAD CSV WITH HEADERS FROM "file:///transactions_ph_1_5a.csv" AS csvLine
MERGE (in:Address {name: 'input', index: csvLine.index, addresses: csvLine.input_key, tx: csvLine.tx, in_id: csvLine.input_index})
MERGE (out:Address {name:'output', index: csvLine.index, addresses: csvLine.output_key, tx: csvLine.tx, out_id: csvLine.output_index})
MERGE (in)-[:PAYS {datetime: csvLine.timestamp, amount: csvLine.satoshis, tx: csvLine.tx}]->(out)
//
MERGE (txs:Transactions {name: 'TX', tx:csvLine.tx})
MERGE (in)-[:PAYS {datetime: csvLine.timestamp, amount: csvLine.satoshis, tx: csvLine.tx}]->(out)
MERGE (out)-[:PAYS{datetime: csvLine.timestamp, amount: csvLine.satoshis, tx: csvLine.tx}]->(in)
//MERGE (txs)-[:PAYS {datetime: csvLine.timestamp, amount: csvLine.satoshis, tx: csvLine.tx}]->(out)
//MERGE (out)-[:PAYS {datetime: csvLine.timestamp, amount: csvLine.satoshis, tx: csvLine.tx}]->(txs)

MATCH (in), (out)
WHERE out.addresses = in.addresses//in.index = out.index
//AND out.addresses = in.addresses
MERGE (out)-[:PAYS{datetime: r.timestamp, amount: r.satoshis, tx: r.tx}]->(in)
MERGE (linknode:link{l:p.tx})
MERGE (out)-[:links]->(linknode)
MERGE (linknode)-[:links]->(in)

MATCH (in)
WITH in.input AS input, COLLECT(in) AS nodelist, COUNT(*) AS count
WHERE count > 1
CALL apoc.refactor.mergeNodes(nodelist) YIELD node
RETURN node

MATCH (in), (out)
WHERE in.input = out.output
REturn in, out
MERGE (in)<-[:PAYS{datetime: csvLine.timestamp, amount: csvLine.satoshis}]-(out)<-[:PAYS{datetime: csvLine.timestamp, amount: csvLine.satoshis}]-(in)
//Returns the transaction walk from seed address level
MATCH (in:input), (out: output)
RETURN in, out
LIMIT 100

//test
MATCH (n:link) RETURN n LIMIT 25

match (n)-[r]->(n2)
with n, [type(r), n2] as relative
return { root: n, relatives: collect(relative) }
limit 100

MATCH p=(:leaf)<-[*]-(r:nonleaf{root:true})
WHERE SINGLE(m IN nodes(p) WHERE exists(m.root) AND m.root=true )
RETURN r
LIMIT 100

START n=node(*)
MATCH (n)<-[:PARENT_OF*]-(root)
WHERE NOT (root)<-[:PARENT_OF]-()
RETURN root
LIMIT 100


MATCH (in)<-[:PAYS]-(out)-[:PAYS]->(in)
WHERE (in)-[:PAYS]->(in)
RETURN in, out
LIMIT 500

MATCH p=(in:input{input:'13AM4VW2dhxYgXeQepoHkHSQuy6NgaEb94'})-[r:PAYS]->(out:output) RETURN p LIMIT 100
MATCH p=(in:input)-[r:PAYS]->(out:output) 
WHERE r.datetime > '2016-04-28 18:19:39+00:00' AND r.datetime < '2016-06-20 20:55:59+00:00'
RETURN p LIMIT 100


//Query to analyse the number of transactions at grouped by year and month
match ()-[pay:PAYS]->()
with  [X IN Split(pay.datetime, '-') | toInteger(X)] as parts
RETURN date({year: parts[0], month: parts[1]}) as yearMonth, count(*) as count
order by yearMonth
limit 100

//Query to analyse the number of nodes that have links between them
match ()-[:PAYS]->()
 return count(*) as count

 //Train sub-graph
MATCH (a)-[r:PAYS]->(b) 
WHERE r.datetime < '2017-05-01'
MERGE (a)-[:PAYS_EARLY {datetime: r.datetime}]-(b);

//Test sub-graph
MATCH (a)-[r:PAYS]->(b) 
WHERE r.datetime >= '2017-05-01'
MERGE (a)-[:PAYS_LATE {datetime: r.datetime}]-(b);

//# negative examples = (# nodes)² - (# relationships) - (# nodes)
MATCH (in:input)
WHERE (in)-[:PAYS_EARLY]-()
MATCH (in)-[:PAYS_EARLY*2..3]-(other)
WHERE not((in)-[:PAYS_EARLY]-(other))
RETURN id(in) AS node1, id(other) AS node2

//Graph Algorithms

//Page Rank
CALL algo.pageRank('BTCaddress', 'PAYS')
CALL algo.pageRank(null, 'PAYS', {
  iterations:20, dampingFactor:0.85, write: true, writeProperty:"pagerank"})

CALL algo.pageRank.stream(null, 'PAYS', {
  iterations:20, dampingFactor:0.85})
YIELD nodeId, score
RETURN algo.asNode(nodeId).index AS output, algo.asNode(nodeId).txid AS tx_is, score
ORDER BY score DESC

//Community Detection
CALL algo.louvain.stream(null, 'PAYS', {})
YIELD nodeId, community
MATCH (n) WHERE id(n)=nodeId
RETURN community,
       avg(size((n)-[:PAYS]->())) as out_degree,
       avg(size((n)<-[:PAYS]-())) as in_degree,
       avg(n.pagerank) as pagerank,
       count(*) as size
ORDER by size DESC limit 10
//MATCH (n:output) where n.index="12t9YDPgwueZ9NyMgw519p7AA8isjr6SMw" RETURN n
//****CLEAN UP
//MATCH (n) DETACH DELETE n
//MATCH (n) REMOVE n.property_key
//---------START AGAIN---------
:param file_name: "file:///12t9YDPgwueZ9NyMgw519p7AA8isjr6SMw_txs_outs.json"
call apoc.load.json($file_name) yield value
UNWIND value.ins as ins
UNWIND value.out as outs
WITH value, ins, outs
MERGE (tx:tx {index:value.txid, depth:value.depth_, time_stamp: apoc.date.format(value.time, 's', 'dd/MM/yyyy HH:mm:ss zzz')})
MERGE (in :output {index: ins.address, label: coalesce(ins.label, "NA")}) 
MERGE (in)-[p:PAYS {time_stamp: apoc.date.format(value.time, 's', 'dd/MM/yyyy HH:mm:ss zzz'), amount: ins.amount, next_tx: ins.next_tx}]->(tx)
MERGE (out :output {index: outs.address, label: coalesce(outs.label, "NA")})
MERGE (tx)-[q:PAYS {time_stamp: apoc.date.format(value.time, 's', 'dd/MM/yyyy HH:mm:ss zzz'), amount: outs.amount, next_tx: coalesce(outs.next_tx, "UNSPENT")}]->(out)

//apoc.date.format(r.time_stamp, 's', 'dd/MM/yyyy HH:mm:ss zzz')

//****POST PROCESS FOR UPDATING DEPTH ON ADDRESS NODES
MERGE (n:output)-[r:PAYS]-(p:tx)
WITH n, COALESCE(n.depth, []) + p.depth AS depth
UNWIND depth as d
WITH n, collect(distinct d) AS unique
set n.depth = unique

//****Label Analysis
match (n) where n.label <> 'NA' return n.depth, n.index, count(n) as numLabel, n.label 
 order by numLabel desc

//****ANALYSIS PHASE
//****Graph algorithm Analysis
//Using pageRank
//Write pageRank as a property on the graph nodes
CALL algo.pageRank()

//***Louvain Community Detection
//***Aggregate results Using out_degree, in_degree and pageRank
CALL algo.louvain.stream(null, null, {})
YIELD nodeId, community
MATCH (n) WHERE id(n)=nodeId
RETURN community,
       avg(size((n)-[:PAYS]->())) as out_degree,
       avg(size((n)<-[:PAYS]-())) as in_degree,
       avg(n.pagerank) as pagerank, count(*) as size
ORDER by size desc

//****Louvain community detection return all nodes
//****export this csv and run into python script for plotting the communities
//****Unsupervised learning of graph embeddings - Include the embedding vector - deepWalk_128 property
//****Include network depth, label, time_stamp properties
//****Export these results to csv for reading into, community detection & PCA + KMeans analysis
CALL algo.louvain.stream(null, null, {})
YIELD nodeId, community
MATCH (n) WHERE id(n)=nodeId
RETURN n.index, community,
       size((n)-[:PAYS]->()) as out_degree,
       size((n)<-[:PAYS]-()) as in_degree,
       n.pagerank as pagerank, n.label, n.depth, n.time_stamp, n.deepWalk_128
ORDER by community asc

//****Deep Walk - Using parameters from the paper - DeepWalk: Online Learning of Social Representations

//****https://arxiv.org/pdf/1403.6652.pdf
Call embedding.deepWalk(null, null, {
  numberOfWalks: 32,
  vectorSize: 128,
  walkLength: 40,
  windowSize: 10,
  writeProperty: "deepWalk_128"
})

//****USE THIS CONSTRUCT AS THE PARAMETERS ARE TUNED FOR THE BITCOIN GRAPHS
//****Write the embedding vectors to the nodes in the graph
Call embedding.deepWalk(null, null, {
  numberOfWalks: 40,
  learningRate: 0.01,
  vectorSize: 64,
  walkLength: 10,
  windowSize: 2,
  writeProperty: "deepWalk_128"
})

//Return deepWalk_128 property
//Export these results to csv for reading into PCA + KMeans analysis
//Unsupervised learning of graph embeddings
MATCH (n) 
WHERE EXISTS(n.deepWalk_128) 
RETURN DISTINCT n.index as entity, n.deepWalk_128 AS deepWalk_128 
UNION ALL MATCH ()-[r]-() WHERE EXISTS(r.deepWalk_128) 
RETURN DISTINCT r.next_tx AS entity, r.deepWalk_128 AS deepWalk_128 
// use limits when testing LIMIT 25

LOAD CSV WITH HEADERS FROM "file:///seed_AnalysisSheet10.csv" AS row 
match (n)-[r]-() where n.index in [row.nodeID] return n, r

//Graph embeddings
//THIS FUNCTION DOES NOT WORK
CALL embedding.deepgl("output","PAYS", {
  nodeFeatures: ['pagerank'],
  iterations: 2
})

 //****Scratch Pad

//Cosine Similarity
MATCH (n)
WITH {item:id(n), weights: n.deepWalk_128} as userData
WITH collect(userData) as data
CALL algo.similarity.cosine.stream(data)
YIELD item1, item2, count1, count2, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY similarity DESC

MATCH (n)
WITH {item:id(n), weights: n.deepWalk_128} as userData
WITH collect(userData) as data
CALL algo.similarity.cosine(data, {topK:10, similarityCutoff: 0.1, write:false, writeRelationshipType: "SIMILAR", 
  writeProperty: "Cosine_sim_score",})
YIELD nodes, similarityPairs
RETURN nodes, similarityPairs

MATCH (n)
WITH {item:id(n), weights: n.deepWalk_128} as userData
WITH collect(userData) as data
CALL algo.similarity.cosine.stream(data, {topK:10, similarityCutoff: 0.1})
YIELD item1, item2, count1, count2, similarity
RETURN algo.asNode(item1).index AS from, algo.asNode(item2).index AS to, similarity
ORDER BY similarity DESC

RETURN nodes, similarityPairs

//YIELD nodes, similarityPairs

//, {similarityCutoff: 0.8, write:true})

MATCH (p:Person), (c:Cuisine)
WITH {item:id(p), weights: collect(coalesce(likes.score, algo.NaN()))} as userData
WITH collect(userData) as data
CALL algo.similarity.cosine.stream(data)
YIELD item1, item2, count1, count2, similarity
RETURN algo.asNode(item1).name AS from, algo.asNode(item2).name AS to, similarity
ORDER BY similarity DESC




 ON CREATE SET q.tx_id=value.txid
//MERGE (tx)-[:inc {i:$i}]->(block)
//SET tx += {tx}    
    
//WITH tx
FOREACH (input in ins |
         MERGE (in :output {index: ins.address}) 
         MERGE (in)-[:PAYS {amount: ins.amount, prev_tx: ins.next_tx}]->(tx)
         )
            
FOREACH (output in outs |
         MERGE (out :output {index: outs.address})
         MERGE (tx)-[:out {amount: outs.amount, next_tx: outs.next_tx}]->(out)
         SET
             out.amount = output.amount,
             tx.txid = output.next_tx,
             //out.scriptPubKey= output.scriptPubKey,
             out.address = output.address
         FOREACH(ignoreMe IN CASE WHEN outs.address <> '' THEN [1] ELSE [] END |
                 MERGE (address :address {address: outs.address})
                 MERGE (out)-[:locked]->(address)
                 )
        )

CALL apoc.merge.relationship(s, row.relation, {}, {}, t) YIELD rel
