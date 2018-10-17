package org.moqui.addons.janusgraph.test
import spock.lang.*
import java.util.concurrent.TimeUnit

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
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.ResultSet
import org.apache.tinkerpop.gremlin.driver.Result

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TestFind extends Specification {
    protected final static Logger logger = LoggerFactory.getLogger(TestFind.class)

    //@Shared
    //Map <String, Object> pciCreateMap = new HashMap()

    EntityValue pci

    EntityValue pci2

    List<Vertex > arr = []
    List<Edge> edgeArr = []

    //org.apache.tinkerpop.gremlin.driver.Client client

    //JanusGraphDatasourceFactory ddf

    @Shared ExecutionContext ec

    @Shared Graph janusGraph

    @Shared Random RANDOM = new Random()


    def setupSpec() {
        // init the framework, get the ec
        logger.info("in TestFind, setupSpec")
        System.out.println("IN TestFind")
        ec = Moqui.getExecutionContext()
    }

    def setup() {
        GraphTraversalSource g = JanusGraphUtils.getTraversalSource (ec) , gts
        org.apache.tinkerpop.gremlin.driver.ResultSet rs
        org.apache.tinkerpop.gremlin.structure.Vertex v
        org.apache.tinkerpop.gremlin.structure.Edge e
        (0..5).eachWithIndex{ int entry, int i ->
            v = g.addV('vPartyContactInfo')
            .property('fullName', 'Create User ${i}')
            .property('emailAddress', 'createUser_${i}@test.com')
            .property('contactNumber', '801-40${i}-5111')
            .property('address1', '1151 ${i} Regent Court')
            .property('city', 'Orem')
            .property('stateProvinceGeoId', 'UT')
            .property('postalCode', '84057')
            .property('sales', new Float(i * 1000000))
            .next()
            logger.info("in TestFind find_PartyContactInfo_list, i: ${i}, v: ${v}")
            arr[i] = v
        }
        String nme
        (1..5).eachWithIndex{ int entry, int i ->
            nme = "testEdge ${i}"
            e = g.V(arr[i].id()).as('a').V(arr[0].id()).addE('testEdge').to('a')
                    .property('name', nme).property('prorate', new Float(i * 10)).next()
            logger.info("in TestFind find_PartyContactInfo_list, i: ${i}, edge: ${e}")
            edgeArr << e
        }

            //ec.user.loginUser("john.doe", "moqui")

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
        //pci = ec.entity.makeValue("vPartyContactInfo")
        pci = new JanusGraphEntityValue(ed, ec.entity, null)
        //logger.info("in TestFind, pciCreateMap: ${pciCreateMap}")
        pci.setAll(pciCreateMap)
        //logger.info("in TestFind, pci(2): ${pci}")

        //pci2 = ec.entity.makeValue("vPartyContactInfo")
        pci2 = new JanusGraphEntityValue(ed, ec.entity, null)
        pci2.setAll(pciCreateMap)
        pci2.set("fullName", "Create User 2")
        pci2.set("contactNumber", "801-555-5111")
        pci2.set("emailAddress", "createUser2@test.com")
        //logger.info("in TestFind, pci(3): ${pci}")

    }

    def cleanupSpec() {
        //janusGraph.close()
        ec.destroy()
    }

    def cleanup() {
        GraphTraversalSource g = JanusGraphUtils.getTraversalSource (ec) , gts
        edgeArr.each {edge ->
            g.E(edge.id()).drop()
        }
        arr.each {vrtx ->
            g.V(vrtx.id()).drop()
        }
    }


    def "find_PartyContactInfo_one"() {
        setup:
        when:
        JanusGraphEntityFind fnd = new JanusGraphEntityFind(ec)
        def result = fnd.condition(arr[0].id(), null, null, null, null).one()
        logger.info("in TestFind, result: ${result}")
        def result2 = fnd.condition(null, null, null, null, [["sales", "gte", new Float(3000000.0)]]).one()
        logger.info("in TestFind, result2: ${result2}")

        then:
        assert result
        assert result2

        cleanup:
        return
    }

    def "find_PartyContactInfo_list"() {

        setup:
        //pci = ec.entity.makeValue("vPartyContactInfo")
        //EntityValue pci

        when:
        JanusGraphEntityFind fnd = new JanusGraphEntityFind(ec)
        Object id = arr[0].id()
        def resultList = fnd.condition(id, "vPartyContactInfo", "testEdge", [["prorate", "gte", new Float(20.0)]], null).list()
        logger.info("in TestFind, resultList: ${resultList}")
        def resultList2 = fnd.condition(arr[0].id(), "vPartyContactInfo", "testEdge", [["prorate", "lte", new Float(40.0)]],
                [["sales", "gte", new Float(3000000.0)]]).list()
        logger.info("in TestFind, resultList2: ${resultList2}")

        then:
        assert resultList && resultList.size() >= 3
        assert resultList2 && resultList2.size() == 2

        cleanup:
        return
    }


}
