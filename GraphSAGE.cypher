//---------Clean up
//MATCH (n) DETACH DELETE n
//MATCH (n) REMOVE n.property_key
//---------1. Load Data & Build Model---------
//---------Changes made from BQ_Graph.cypher script
//---------Added 'in_amount' and 'out_amount' for more granular analysis
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

//---------2. POST PROCESS FOR UPDATING DEPTH ON ADDRESS NODES---------
MERGE (n:output)-[r:PAYS]-(p:tx)
WITH n, COALESCE(n.depth, []) + p.depth AS depth
UNWIND depth as d
WITH n, collect(distinct d) AS unique
set n.depth = unique

//---------2.a. PROPERTY KEYS YOU WISH TO USE AS FEATURES IN THE GRAPH CATALOGUE FOR GDS ALGORITHMS 
//---------YOU MUST CAST DATA TYPES NEEDED
//---------NEO4j doesn't currently support strings as properties in GDS. Because none of the algorithms use strings as input values.
//---------https://community.neo4j.com/t/gds-graph-create-doesnt-support-string-for-nodeproperties-data-type/30217
//---------THEREFORE, FEATURES THAT CAN BE USED IN THE GRAPH CATALOGUE MUST BE OF TYPE:
//---------https://neo4j.com/docs/graph-data-science/current/management-ops/node-properties/
//-----------Long - Long.MIN_VALUE
//-----------Double - NaN
//-----------Long Array - null
//-----------Float Array - null
//-----------Double Array - null
//---------APOC conversions: https://neo4j.com/labs/apoc/4.1/overview/apoc.convert/apoc.convert.toIntList/ 
//---------https://neo4j.com/labs/apoc/4.1/data-structures/conversion-functions/ 
//--
//---------2.a.i.casting the 'depth' property into a Long Array
MATCH (n)
SET n.depth = apoc.convert.toIntList(n.depth)

//---------DATE AND TIME CONVERSIONS
//---------https://neo4j.com/labs/apoc/4.1/temporal/datetime-conversions/
//---------CONVERT the 'time_stamp' property INTO MILLIS for use in the GDS algorithms

//---------return apoc.date.parse('04/08/2017 12:21:57 UTC','ms', 'dd/MM/yyyy HH:mm:ss zzz') as outputmillis
//--
//---------2.a.ii.Need to update the transaction nodes (tx) and the payment relationships [:PAYS]
MATCH ()-[r:PAYS]->()
SET r.time_stamp = apoc.date.parse(r.time_stamp,'ms', 'dd/MM/yyyy HH:mm:ss zzz')

MATCH (tx:tx)
SET tx.time_stamp = apoc.date.parse(tx.time_stamp,'ms', 'dd/MM/yyyy HH:mm:ss zzz')
//---------

//---------CONVERT the timestamp from millis BACK into original date format
//return apoc.date.format(1501849317000, "ms", "dd/MM/yyyy HH:mm:ss zzz") AS output;
//---------
MATCH ()-[r:PAYS]->()
SET r.time_stamp = apoc.date.format(r.time_stamp,'ms', 'dd/MM/yyyy HH:mm:ss zzz')

MATCH (tx:tx)
SET tx.time_stamp = apoc.date.format(tx.time_stamp,'ms', 'dd/MM/yyyy HH:mm:ss zzz')
//---------

//---------One hot encoding - encoding the position in time of the transactions in the set of addresses involved in the graph
//---------set this encoding as a property on the address nodes for use in GDS algorithms
//---------!!!NOT REQUIRED AT THE MOMENT --10.03.21
//MATCH (p:tx)
//WITH p
//  ORDER BY p.time_stamp
//WITH collect(p) AS p1
//MATCH (output:output)
//SET output.onehotencode = gds.alpha.ml.oneHotEncoding(p1, [(output)-[:PAYS]-(p) | p])
//RETURN output.index AS name, output.onehotencode
//  ORDER BY name

//MATCH (output:output)
//WITH output
//  ORDER BY output.pageRank
//WITH collect(output) AS outputs
//MATCH (p:tx)
//SET p.onehotencode_tx = gds.alpha.ml.oneHotEncoding(outputs, [(p)-[:PAYS]-(output) | output])
//RETURN p.index AS name, p.onehotencode_tx
//  ORDER BY name
//---------
//---------Graph Catalog
//Graph algorithms run on a graph data model which is a projection of the Neo4j property graph data model. 
//A graph projection can be seen as a view over the stored graph, containing only analytically relevant, potentially aggregated, topological and property information. 
//Graph projections are stored entirely in-memory using compressed data structures optimized for topology and property lookup operations.
//The graph catalog is a concept within the GDS library that allows managing multiple graph projections by name. 
//Using its name, a created graph can be used many times in the analytical workflow. Named graphs can be created using either a Native projection or a Cypher projection. 
//After usage, named graphs can be removed from the catalog to free up main memory.
//Graphs can also be created when running an algorithm without placing them in the catalog. We refer to such graphs as anonymous graphs.

//****Label Analysis - Used for Chapter 4/5 Publication
//match (n) where n.label <> 'NA' return n.depth, n.index, count(n) as numLabel, n.label 
// order by numLabel desc

//---------3. GRAPH ANALYSIS PHASE
//---------CENTRALITY ALGORITHMS USED AS FEATURES: https://neo4j.com/docs/graph-data-science/current/algorithms/centrality/
//---------3.a. USING pageRank - Write pageRank as a property on the graph nodes
//---------3.a.i Create the Graph Catalogue---------
CALL gds.graph.create('myGraph', ['output', 'tx'], '*')
YIELD graphName, nodeCount, relationshipCount;


CALL gds.pageRank.write('myGraph', {
  maxIterations: 20,
  dampingFactor: 0.85,
  writeProperty: 'pageRank'
})

//------------Betweenness Centrality -- NOT USED IN THIS CASE
//------------https://neo4j.com/docs/graph-data-science/current/algorithms/betweenness-centrality/
//------------
//CALL gds.betweenness.stream('myGraph')
//YIELD nodeId, score
//RETURN gds.util.asNode(nodeId).index AS name, score
//ORDER BY name ASC

//------------PATH FINDING ALGORITHMS -- NOT USED IN THIS CASE
//------------https://neo4j.com/docs/graph-data-science/current/algorithms/yens/
//Lets find the k paths from the ransomware seed address to its cash out destination hits an exchange
//in this case 'HitBTC.com' and 'Poloniex.com' (1BvTQTP5PJVCEz7dCU2YxgMskMxxikSruM)
//MATCH (source:output {index: '12t9YDPgwueZ9NyMgw519p7AA8isjr6SMw'}), (target:output {index: '1ETWkyQUY9nRpVMyGwha4vRhwKgMbomMQe'})
//CALL gds.beta.shortestPath.yens.stream('addresses_with_transactions_1', {
//    sourceNode: id(source),
//    targetNode: id(target),
//    k: 3,
//    relationshipWeightProperty: 'amount'
//})
//YIELD index, sourceNode, targetNode, totalCost, nodeIds, costs
//RETURN
//    index,
//    gds.util.asNode(sourceNode).index AS sourceNodeName,
//    gds.util.asNode(targetNode).index AS targetNodeName,
//    totalCost,
//    [nodeId IN nodeIds | gds.util.asNode(nodeId).index] AS nodeNames,
//    costs
//ORDER BY index
//------------Louvain Community Detection - Aggregate results Using out_degree, in_degree and pageRank
//--NOTE: alogorithm returns strange 'communityId'
//CALL gds.louvain.stream('myGraph')
//YIELD nodeId, communityId
//MATCH (n) WHERE id(n)=nodeId
//RETURN communityId,
//       avg(size((n)-[:PAYS]->())) as out_degree,
//       avg(size((n)<-[:PAYS]-())) as in_degree,
//       avg(n.pageRank) as pagerank, count(*) as size
//ORDER by size desc

//------------Louvain community detection return all nodes
//------------export this csv and run into python script for plotting the communities
//------------Unsupervised learning of graph embeddings - Include the embedding vector - deepWalk_128 property
//------------Include network depth, label, time_stamp properties
//------------Export these results to csv for reading into, community detection & PCA + KMeans analysis
//--LOUVAIN NOT USED
//CALL gds.louvain.stream('myGraph')
//YIELD nodeId, communityId
//MATCH (n) WHERE id(n)=nodeId
//RETURN n.index, communityId,
//       size((n)-[:PAYS]->()) as out_degree,
//       size((n)<-[:PAYS]-()) as in_degree,
//       n.pagerank as pagerank, n.label, n.depth, n.time_stamp
//ORDER by communityId asc
//{ writeProperty: 'community' }
//--
//------------3.b.i. SET PROPERTIES FOR DEGREE CENTRALITY (IN / OUT DEGREE)
MATCH (n)
SET n.out_degree = size((n)-[:PAYS]->())
SET n.in_degree = size((n)<-[:PAYS]-())
//--
RETURN n.index,
       n.out_degree, n.in_degree, n.pageRank as pagerank, n.label, n.depth, n.time_stamp
ORDER by pagerank asc

//------------3.b.ii. SET PROPERTIES ON 'TX' NODE FOR sum(in.amount) AND sum(out.amount)
//------------REMOVE THIS FOR NOW (12.03.21) USE THE AMOUNT ON THE RELATIONSHIP AS WEIGHT IN TRAINING
//------------TOTAL AMOUNT PASSING THROUGH THE TX NODE (IN + OUT)
MATCH (n:output)-[r:PAYS]-(q:tx)
WITH q, sum(r.amount) as total_btc
SET q.total_amount = total_btc
RETURN q.index as txid, total_btc
//------------TOTAL AMOUNT PASSING THROUGH THE ADDRESS NODE (IN + OUT)
MATCH (n:output)-[r:PAYS]-(q:tx)
WITH n, sum(r.amount) as total_btc
SET n.total_amount = total_btc
RETURN n.index as btc_addr, total_btc
//--TOTAL IN AMOUNT
//MATCH (n:output)-[r:PAYS]->(q:tx)
//WITH q, sum(r.in_amount) as total_in
//SET q.total_in_amount = total_in
//RETURN q.index as txid, total_in
//--TEST
//WHERE q.index = '131551e35e7a644b76ea5366f744313bff3f959207c416f7b7b7f9b1cc90b0a3'
//RETURN q.index as transaction, sum(r.in_amount) as total_in_amount
//--TOTAL OUT AMOUNT 
//MATCH (n:output)<-[r:PAYS]-(q:tx)
//WITH q, sum(r.out_amount) as total_out
//SET q.total_out_amount = total_out
//RETURN q.index as txid, total_out
//--TEST
//WHERE q.index = '131551e35e7a644b76ea5366f744313bff3f959207c416f7b7b7f9b1cc90b0a3'
//RETURN q.index as transaction, sum(r.out_amount) as total_out_amount

//{ writeProperty: 'community' }
//------------3.b.iii. Calculate risk rating
//--OLD CALC--Exposure = (((in+out)/total sum in and out)xavgBTCovernetwork)/pageRank)
//------------NEW CALC -- frequency x severity
//------------frequency = sum(in_degree + out_degree)[at each node]/total sum of in_degree and out_degree over the entire sampled network
//------------severity = total amount of BTC moving through the node (address and transaction)
//------------will need to calucluate the rating for address nodes and transaction nodes
//((sum(n.total_in_amount)+sum(n.total_out_amount))/sum(n.in_degree)) as avgBTC
//--TX NODE
MATCH (n)
WITH sum(n.in_degree+n.out_degree) as total_degrees
MATCH (q:tx)
WITH q,(q.in_degree+q.out_degree) as a, total_degrees as td1, q.total_amount as x
SET q.risk_rating = ((apoc.convert.toFloat(a))/(apoc.convert.toFloat(td1))*x) 
RETURN q.index, q.risk_rating as risk_rating
//--ADDR NODE
MATCH (n)
WITH sum(n.in_degree+n.out_degree) as total_degrees
MATCH (q:output)
WITH q,(q.in_degree+q.out_degree) as a, total_degrees as td1, q.total_amount as x
SET q.risk_rating = ((apoc.convert.toFloat(a))/(apoc.convert.toFloat(td1))*x) 
RETURN q.index, q.risk_rating as risk_rating


//------------3.c. Graph Embeddings 
//------------DEEP WALK ALGORITHM - NOT USED HERE
//------------Ref: DeepWalk: Online Learning of Social Representations - https://arxiv.org/pdf/1403.6652.pdf
//------------Using parameters from the paper
//Call embedding.deepWalk(null, null, {
//  numberOfWalks: 32,
//  vectorSize: 128,
//  walkLength: 40,
//  windowSize: 10,
//  writeProperty: "deepWalk_128"
//})

//------------3.c. Graph Embeddings
//------------GRAPH SAGE - http://snap.stanford.edu/graphsage/ 
//------------Ref: Inductive Representation Learning on Large Graphs - https://arxiv.org/pdf/1706.02216.pdf
//------------Representation Learning on Graphs: Methods and Applications - https://arxiv.org/pdf/1709.05584.pdf
//------------Git - https://github.com/williamleif/GraphSAGE
//------------Default parameters taken from: https://neo4j.com/docs/graph-data-science/1.3-preview/algorithms/alpha/graph-sage/ 
//------------BETA release : https://neo4j.com/docs/graph-data-science/current/algorithms/graph-sage/
//------------https://neo4j.com/developer/graph-data-science/graph-embeddings/ 

//CALL gds.alpha.graphSage.write(
//writeProperty: 'graphSAGEembedding',

//------------3.c.i CREATE ANOTHER GRAPH CATALOG - TO TRAIN THE GRAPH SAGE MODEL
//--
//------------Addresses with transactions
//------------make sure the data types are the same
//------------be mindful of the fact that not all properties exist on each node label and maybe projected as 0 values
//------------the same properties are required on each node label for the model to train
//------------Node properties MUST BE present for each label in the graph: Example: [exposure, time_stamp, total_in_amount
//------------total_out_amount]. Properties that exist for each label are [in_degree, pageRank, out_degree]
CALL gds.graph.create(
    'addresses_with_transactions_1', {
        output: {
                label: 'output',
                properties: {
                    risk_rating: {
                        property: 'risk_rating',
                        defaultValue: 0.0
                    },
                    pageRank: {
                        property: 'pageRank',
                        defaultValue: 0
                    },
                    in_degree: {
                        property: 'in_degree',
                        defaultValue: 0
                    },
                    out_degree: {
                        property: 'out_degree',
                        defaultValue: 0
                    },
                    time_stamp: {
                        property: 'time_stamp',
                        defaultValue: 0
                    },
                    total_amount: {
                        property: 'total_amount',
                        defaultValue: 0.0
                    }
                }
            },
        tx: {
                label: 'tx',
                properties: {
                    risk_rating: {
                        property: 'risk_rating',
                        defaultValue: 0.0
                    },
                    pageRank: {
                        property: 'pageRank',
                        defaultValue: 0
                    },
                    in_degree: {
                        property: 'in_degree',
                        defaultValue: 0
                    },
                    out_degree: {
                        property: 'out_degree',
                        defaultValue: 0
                    },
                    time_stamp: {
                        property: 'time_stamp',
                        defaultValue: 0
                    },
                    total_amount: {
                        property: 'total_amount',
                        defaultValue: 0.0
                    }
                }
            }
    }, {
    PAYS: {
        type: 'PAYS',
        orientation: 'NATURAL',
        properties: {
            amount: {
                property: 'amount',
                defaultValue: 0.0
            },
            time_stamp: {
                property: 'time_stamp',
                defaultValue: 0
        }
      }
    }
  }
)
YIELD graphName, nodeCount, relationshipCount;


//NOT using relationship weights - in this case the amount of the transaction
//If you strip out the features with the longArray data type and just leave one node 'output' with feature 'pageRank' 
//then the model can be trained
//Community users can only store one model in the catalog

///------------C.ii. TRAIN THE MODEL
//------------output to the file 20210311_GraphSAGE_embeddings.csv
//------------output to the file 20210313a_GraphSAGE_embeddings.csv 
//--------------removed 'time_stamp' from featureProperties and added relationship weight 'amount'
CALL gds.beta.graphSage.train(
  'addresses_with_transactions_1',
  {
    modelName: 'weightedTrainedModel',
    featureProperties: ['pageRank', 'risk_rating', 'in_degree', 'out_degree', 'total_amount'],
    aggregator: 'mean',
    activationFunction: 'sigmoid',
    sampleSizes: [25, 10],
    degreeAsProperty: true,
    relationshipWeightProperty: 'amount',
    relationshipTypes: ['PAYS']
  }
)
//nodeLabels: ['tx']
//------------Testing different hyperparameters
//------------Community edition we must drop the current model before training another
//------------20210313_GraphSAGE_embed_features.csv file created
//------------output to the file 20210313b_GraphSAGE_embeddings.csv 
//--------------removed 'time_stamp' from featureProperties
CALL gds.beta.graphSage.train('addresses_with_transactions_1',{
  modelName:'testModel',
  aggregator:'pool',
  batchSize:512,
  activationFunction:'relu',
  epochs:10,
  sampleSizes:[25,10],
  learningRate:0.0000001,
  embeddingDimension:256,
  featureProperties:['pageRank', 'risk_rating', 'in_degree', 'out_degree', 'total_amount']})
YIELD modelInfo
RETURN modelInfo

//--------------Trying the FastRP embedding algorithm
//--------------https://neo4j.com/developer/graph-data-science/node-classification/
//--------------https://neo4j.com/docs/graph-data-science/current/algorithms/fastrp/#algorithms-embeddings-fastrp-examples-stream

CALL gds.beta.fastRPExtended.stream('addresses_with_transactions_1',{
    relationshipTypes:['PAYS'],
    featureProperties: ['pageRank', 'risk_rating', 'in_degree', 'out_degree', 'total_amount'], //5 node features
    relationshipWeightProperty: 'amount',
    propertyDimension: 45,
    embeddingDimension: 250,
    iterationWeights: [0, 0, 1.0, 1.0],
    normalizationStrength:0.05
    //writeProperty: 'fastRP_Extended_Embedding'
})
YIELD nodeId, embedding
RETURN gds.util.asNode(nodeId).index as name, gds.util.asNode(nodeId).risk_rating as exp, gds.util.asNode(nodeId).pageRank as pr, gds.util.asNode(nodeId).out_degree as outdeg, gds.util.asNode(nodeId).in_degree as indeg, gds.util.asNode(nodeId).total_amount as ta, gds.util.asNode(nodeId).time_stamp as ts, embedding as features

//------------C.iii. STREAM the embeddings
CALL gds.beta.graphSage.stream(
  'addresses_with_transactions_1',
  {
    modelName: 'weightedTrainedModel'
  }
)
//------------STREAM embeddings with respective properties
//------------output to the file 20210311_GraphSAGE_embeddings.csv
CALL gds.beta.graphSage.stream(
  'addresses_with_transactions_1',
  {
    modelName: 'testModel'
  }
)
YIELD nodeId, embedding
RETURN gds.util.asNode(nodeId).index as name, gds.util.asNode(nodeId).risk_rating as exp, gds.util.asNode(nodeId).pageRank as pr, gds.util.asNode(nodeId).out_degree as outdeg, gds.util.asNode(nodeId).in_degree as indeg, gds.util.asNode(nodeId).total_amount as ta, gds.util.asNode(nodeId).time_stamp as ts, embedding as features

//------------**********************------------//
//------------ADMIN FUNCTIONS
//--which samples the database and outputs schema for all labels & rel types. 
CALL apoc.meta.nodeTypeProperties() 
CALL apoc.meta.relTypeProperties() 

CALL dbms.procedures()

CALL gds.graph.list('rnsware')

//drop a graph catalogue
CALL gds.graph.drop('tx_subgraph') YIELD graphName;

//drop a model - can only use one model at a time in community edition 
CALL gds.beta.model.drop('my-model')
YIELD
  modelInfo,
  loaded,
  stored,
  shared

//------------RAW JSON STRUCTURE OF GRAPH DATA 
"labels": [
      "output"
    ],
"properties": {
"index": "1P2SbiV5zKAwMTZH1VdExXM2sXRjkCeTsx",
"pageRank": 1.3099369883325493,
"depth": [
        "3",
        "2",
        "1"
      ],
"label": "NA"
    }
  },
  "end": {
"identity": 0,
"labels": [
      "tx"
    ],
"properties": {
"index": "3332d270983f3183af866714b8eb4ad226f4f4bea2ce42efcfd2de2dfdaf0f12",
"pageRank": 1.0959224477219613,
"depth": "3",
"time_stamp": "04/08/2017 12:21:57 UTC"
    }
  },
//relationship
  "type": "PAYS",
"properties": {
"next_tx": "c7eb28c30d8b23e0612b1678a2ca1cd879655eda3e9f190ea3f6f67a176e475d",
"amount": 0.12962436,
"time_stamp": "04/08/2017 12:21:57 UTC"
        }
      },
match (n)
where n.index in [
'12t9YDPgwueZ9NyMgw519p7AA8isjr6SMw',
'14MuKrmk6dTdrwsdvgYZGbN8Km6F1i53zd',
'1589f5d03ee14227c84a4e02abd9c0956ff3636f2e53491d5a2004e59ba65e5c',
'19UyfTi6hv8CGTVP62DcLu4EmyBuihQiNy',
'1BvTQTP5PJVCEz7dCU2YxgMskMxxikSruM',
'1CBNoYPstMXsMi6nPfobCbyPZh73fn6SSh',
'1Dha5e1jbTtu4YGALQ3DnfTAk5yxzm4XSR',
'1Ej4Jm8J83tKG4wUAbikNF3rQoGckH4Emp',
'1ETWkyQUY9nRpVMyGwha4vRhwKgMbomMQe',
'1GTFUbGUSBiiXzferF9DbA1V6VZB38w2Nz',
'1Jkp6RupCtoRvKkAtAh8yRtNJoi8ogMtie',
'1NL7G1kwto8REg5TPdEtwHNRDtwbvQdCpF',
'1P2SbiV5zKAwMTZH1VdExXM2sXRjkCeTsx',
'29779df2e2a5a1f823b22e7e974a0082bdfd389edc1c11d1d4f6b290d8118d27',
'35e5d5fe8c8128cfa6884f56be5817e4138c58c91b79d78d3e78a8d365b9d8a7',
'36ef488e59d719fb906254aed61bfe46e8f64778bc6cac97e56a68c241004c28',
'36vB6ZvEZaTAvpmTKa2wXLWW5mz4ACuZ7y',
'409803bb5e124fd028c0482027c7722e84ce55b78204b279d3a44aba5e7c1698',
'5c181889994d707ee0a237ecf62efb53d532aa2b6077bf02b6eaa7a165784f9f',
'6fc639ba056de897d32c26cc2f5a917dfb38256eef5e92244edf06284cd82ab0',
'c03e48ad9fc778170c86542c0414a89052b21679a3576121ca6b1c2d340f1e22',
'c371eb6820214043060538ef4f79b796607c63f73b9a74bf6a4fdf1c1c63ef19']
return n