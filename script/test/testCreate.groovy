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
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.graphdb.database.StandardJanusGraph

org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("testCreate.groovy")
logger.info("in testCreate.groovy")

EntityValue pci = ec.entity.makeValue("vPartyContactInfo")
logger.info("in testCreate.groovy, pci: ${pci}")

Map <String, Object> pciMap = new HashMap<String, Object>([fullName: "John Doe"])
pci.setAll(pciMap)
logger.info("in testCreate.groovy, pci(2): ${pci}")
EntityValue pci2 = pci.create()
logger.info("in testCreate.groovy, pci2: ${pci2}")
StandardJanusGraph janusGraph = pci.getDatabase()
GraphTraversalSource g = janusGraph.traversal()
logger.info("in testCreate.groovy, val: ${val}")
GraphTraversal val = g.V(pci.getId())
logger.info("in testCreate.groovy, val: ${val}")
logger.info("in testCreate.groovy, valueMap: ${pci.getValueMap()}")
