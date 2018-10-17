package org.moqui.addons.janusgraph.test
import spock.lang.*
import java.sql.Timestamp

import org.moqui.context.ExecutionContext
import org.moqui.entity.EntityValue
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.Moqui

import org.moqui.addons.janusgraph.JanusGraphDatasourceFactory
import org.moqui.addons.janusgraph.JanusGraphEntityValue
import org.moqui.addons.janusgraph.JanusGraphEntityFind
import org.moqui.addons.janusgraph.JanusGraphUtils

import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.core.JanusGraph
import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.core.VertexLabel
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.EdgeLabel
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.graphdb.vertices.StandardVertex
import org.janusgraph.core.JanusGraphTransaction
import org.janusgraph.core.JanusGraphVertex

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Direction
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.ResultSet

import org.moqui.addons.janusgraph.JanusGraphUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TestStoreVertexAndEdge extends Specification {
    protected final static Logger logger = LoggerFactory.getLogger(Moqui.class)

    //@Shared
    //Map <String, Object> pciCreateMap = new HashMap()

    @Shared
    EntityValue pci

    @Shared
    EntityValue pci2

    @Shared
    ExecutionContext ec

    @Shared
    Graph janusGraph

    @Shared
    Timestamp nowTimestamp

    //org.apache.tinkerpop.gremlin.driver.Client client

    def setupSpec() {
        // init the framework, get the ec
        logger.info("in TestStoreVertexAndEdge, setupSpec")
        System.out.println("IN TestStoreVertexAndEdge")
        ec = Moqui.getExecutionContext()
        janusGraph = JanusGraphUtils.getDatabase(ec)

        nowTimestamp = ec.user.nowTimestamp
        Map <String, Object> pciCreateMap = new HashMap()
        pciCreateMap.put("fullName", "Create User")
        pciCreateMap.put("emailAddress", "createUser@test.com")
        pciCreateMap.put("contactNumber", "801-400-5111")
        pciCreateMap.put("address1", "1151 Regent Court")
        pciCreateMap.put("city", "Orem")
        pciCreateMap.put("stateProvinceGeoId", "UT")
        pciCreateMap.put("postalCode", "84057")

        //pci = ec.entity.makeValue("vPartyContactInfo")
        EntityDefinition ed = ec.entity.getEntityDefinition("vPartyContactInfo")
        pci = new JanusGraphEntityValue(ed, ec.entity, null)
        logger.info("in TestStoreVertexAndEdge, pciCreateMap: ${pciCreateMap}")
        pci.setAll(pciCreateMap)
        logger.info("in TestStoreVertexAndEdge, pci(2): ${pci}")

        //pci2 = ec.entity.makeValue("vPartyContactInfo")
        pci2 = new JanusGraphEntityValue(ed, ec.entity, null)
        pci2.set("contactNumber", "801-555-5111")
        pci2.set("emailAddress", "createUser2@test.com")
        pci2.stateProvinceGeoId = "USA_CA"
        pci2.fullName = "CA User"
        pci2.emailAddress = "CA" + "_State@test.com"
        pci2.lastUpdatedStamp = nowTimestamp.getTime()
        logger.info("in TestStoreVertexAndEdge, pci2(2): ${pci2}")

    }

    def cleanupSpec() {
        //janusGraph.close()
        ec.destroy()
        return
    }

    def setup() {
        //client = JanusGraphUtils.getClient(ec)
        return
    }

    def cleanup() {
        //client.close()
        return
    }

    def "get_root_vertex"() {
        when:
        org.apache.tinkerpop.gremlin.structure.Vertex v=JanusGraphUtils.getRootVertex(ec)

        then:
        assert v
    }

    def "test_storeVertexAndEdge"() {

        setup:
        GraphTraversalSource g = JanusGraphUtils.getTraversalSource (ec)

        when:
        pci = pci.create()
        logger.info("in test_storeVertexAndEdge, pci (3): ${pci}")
        Map<String,Object> edgeProps = ["statusId":"pPPActive",
                                        "fromRoleTypeId": "agent",
                                        "createdDate": nowTimestamp.getTime(),
                                        "lastUpdatedStamp": nowTimestamp.getTime()
        ]

        Map<String,Object> vertexProps = [
                                        "createdDate": nowTimestamp.getTime(),
                                        "lastUpdatedStamp": nowTimestamp.getTime(),
                                        "label": 'vPartyContactInfo'
        ]

        EntityValue testEntityValue = JanusGraphUtils.storeVertexAndEdge(
                pci.getVertex(), null, "agent-client", edgeProps, vertexProps, null, ec
        )

        org.apache.tinkerpop.gremlin.structure.Vertex vrtx = g.V(testEntityValue.getVertex().id()).next()
        logger.info("in TestStoreVertexAndEdge,  vrtx: ${vrtx}")

        then:
        assert vrtx
        return

        cleanup:
        g.close()
    }

}
