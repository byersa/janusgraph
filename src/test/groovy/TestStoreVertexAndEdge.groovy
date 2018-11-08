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

    @Shared
    Map <String,Object> retMap

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
        ec && ec.destroy()
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

    def "test_storeVertexAndEdge_create"() {

        setup:
        GraphTraversalSource g = JanusGraphUtils.getTraversalSource (ec)

        when:
        pci = pci.create()
        logger.info("in test_storeVertexAndEdge, pci (3): ${pci}")
        logger.info("in test_storeVertexAndEdge, pci (4)pci.getVertex: ${pci.getVertex()}")
        Map<String,Object> edgeProps = ["statusId":"pPPActive",
                                        "fromRoleTypeId": "agent"
                                        //"createdDate": nowTimestamp.getTime(),
                                        //"lastUpdatedStamp": nowTimestamp.getTime()
        ]

        Map<String,Object> vertexProps = [
                //"createdDate": nowTimestamp.getTime(),
                //"lastUpdatedStamp": nowTimestamp.getTime(),
                "address1": '1151b Center St',
                "emailAddress": '1151b@test.com',
                "_label": 'vPartyContactInfo'
        ]

        retMap = JanusGraphUtils.storeVertexAndEdge(
                pci.getVertex(), null, "agent-client", edgeProps, vertexProps, null, ec
        )

        logger.info("in TestStoreVertexAndEdge,  retMap: ${retMap}")
        org.apache.tinkerpop.gremlin.structure.Vertex vrtx = retMap.vertex
        logger.info("in TestStoreVertexAndEdge,  vrtx: ${vrtx}")

        then:
        assert vrtx
        return

        cleanup:
        g.close()
    }

    def "test_storeVertexAndEdge_update"() {

        setup:
        GraphTraversalSource g = JanusGraphUtils.getTraversalSource (ec)
        String emailAddr

        when:
        pci = retMap.entity
        logger.info("in test_storeVertexAndEdge_update, pci (5): ${pci}")
        logger.info("in test_storeVertexAndEdge_update, pci (6)pci.getVertex: ${pci.getVertex()}")
        String newEmailAddr = '1151c@test.com'
        Map<String,Object> edgeProps = ["statusId":"pPPInactive2",
                                        "fromRoleTypeId": "buyer2"
                                        //"createdDate": nowTimestamp.getTime(),
                                        //"lastUpdatedStamp": nowTimestamp.getTime()
        ]

        Map<String,Object> vertexProps = [
                                        //"createdDate": nowTimestamp.getTime(),
                                        //"lastUpdatedStamp": nowTimestamp.getTime(),
                                        "address1": '1151c Center St',
                                        "emailAddress": newEmailAddr
        ]

        org.apache.tinkerpop.gremlin.structure.Vertex toVertex = retMap.vertex
        EntityValue toEntityValue = retMap.entity

        retMap = JanusGraphUtils.storeVertexAndEdge(
                pci.getVertex(), toEntityValue, "agent-client", edgeProps, vertexProps, g, ec
        )

        logger.info("in TestStoreVertexAndEdge_update,  retMap: ${retMap}")
        org.apache.tinkerpop.gremlin.structure.Vertex vrtx = retMap.vertex
        logger.info("in TestStoreVertexAndEdge_update,  vrtx: ${vrtx}")
        emailAddr = vrtx.property('emailAddress').value()
        logger.info("in TestStoreVertexAndEdge_update,  emailAddr: ${emailAddr},  newEmailAddr: ${newEmailAddr}")
        then:
        assert emailAddr == newEmailAddr
        return

        cleanup:
        pci.getVertex().remove()
        g.close()
    }

}
