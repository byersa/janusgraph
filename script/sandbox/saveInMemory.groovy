import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.entity.EntityFind
import org.moqui.impl.entity.EntityFindImpl
import org.moqui.entity.EntityValue
import org.moqui.impl.entity.EntityValueImpl
import org.moqui.entity.EntityException
import groovy.json.JsonSlurper

import groovy.json.JsonBuilder

import org.moqui.addons.graph.JanusGraphEntityValue
import org.moqui.addons.graph.JanusGraphUtils

import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.core.JanusGraph
import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.core.VertexLabel
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.EdgeLabel
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import  org.apache.tinkerpop.gremlin.structure.io.IoCore
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.moqui.addons.graph.JanusGraphDatasourceFactory

org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("sandbox.groovy")
logger.info("in sandbox.groovy")

logger.info("in sandbox.groovy, ec: ${ec}")
JanusGraphDatasourceFactory edfi = ec.entity.getDatasourceFactory("transactional_nosql") as JanusGraphDatasourceFactory
StandardJanusGraph janusGraph = edfi.getDatabase()
logger.info("in sandbox.groovy, janusGraph: ${janusGraph}")
logger.info("in sandbox.groovy, IoCore: ${IoCore}")
janusGraph.io(IoCore.graphson()).writeGraph('ofnan-graph.json')
//janusGraph.io(IoCore.graphml()).writeGraph('ofnan-graph.xml')
