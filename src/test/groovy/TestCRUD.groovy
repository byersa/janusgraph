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
//import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.ResultSet
import org.apache.tinkerpop.gremlin.driver.Result

import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TestCRUD extends Specification {
    protected final static Logger logger = LoggerFactory.getLogger(Moqui.class)

    //@Shared
    //Map <String, Object> pciCreateMap = new HashMap()

    EntityValue pci

    EntityValue pci2

    @Shared ExecutionContext ec

    @Shared Graph janusGraph

    @Shared Random RANDOM = new Random()

    //org.apache.tinkerpop.gremlin.driver.Client client

    def setupSpec() {
        // init the framework, get the ec
        logger.info("in TestCRUD, setupSpec")
        System.out.println("IN TestCRUD")
        ec = Moqui.getExecutionContext()
        janusGraph = JanusGraphUtils.getDatabase(ec)
    }

    def setup() {
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
        //logger.info("in TestCRUD, pciCreateMap: ${pciCreateMap}")
        pci.setAll(pciCreateMap)
        //logger.info("in TestCRUD, pci(2): ${pci}")

        //pci2 = ec.entity.makeValue("vPartyContactInfo")
        pci2 = new JanusGraphEntityValue(ed, ec.entity, null)
        pci2.setAll(pciCreateMap)
        pci2.set("fullName", "Create User 2")
        pci2.set("contactNumber", "801-555-5111")
        pci2.set("emailAddress", "createUser2@test.com")
        //logger.info("in TestCRUD, pci(2): ${pci}")

        return
    }

    def cleanupSpec() {
        //janusGraph.close()
        ec.destroy()
        return
    }

    def cleanup() {
        return
    }

//    def "get_root_vertex"() {
//        setup:
//        logger.info("in TestCRUD, get_root_vertex")
//
//        when:
//        org.apache.tinkerpop.gremlin.structure.Vertex v=JanusGraphUtils.getRootVertex(ec)
//        logger.info("in TestCRUD, get_root_vertex, v: ${v}")
//
//        then:
//        assert v
//    }
//
    def "create_PartyContactInfo"() {

        setup:
        GraphTraversalSource g = JanusGraphUtils.getTraversalSource (ec) , gts
        Object id
        org.apache.tinkerpop.gremlin.structure.Vertex v, v2


        logger.info("in TestCRUD, create_PartyContactInfo, g: ${g}")

        when:
        //JanusGraphTransaction tx = janusGraph.buildTransaction().start()
        try {

        logger.info("in TestCRUD, create_PartyContactInfo, pci (1): ${pci}")
        pci = pci.create()
        logger.info("in TestCRUD, create_PartyContactInfo, pci (1b): ${pci}")


        v = pci.getVertex()
        logger.info("in TestCRUD, create_PartyContactInfo, v: ${v}")
        id = v.id()
        logger.info("in TestCRUD, create_PartyContactInfo, pci.id: ${id}")
        v2 = g.V(id).next()
        logger.info("in TestCRUD, create_PartyContactInfo, v2.id(): ${v2.id()}")
        logger.info("in TestCRUD, create_PartyContactInfo, id == v2.id(): ${id == v2.id()}")
        } catch (Exception e) {
            String msg = e.getMessage()
            logger.info("in TestCRUD, create_PartyContactInfo, exception: ${msg}")
        }

        then:
        id == v2.id()

        cleanup:
        g.V('${id}').drop()
        g.close()
    }

    def "update_PartyContactInfo"(){
        setup:
        GraphTraversalSource g = JanusGraphUtils.getTraversalSource (ec) , gts
        String emailAddress, updatedEmail, propEmail
        Object id

        when:
        //JanusGraphTransaction tx = janusGraph.buildTransaction().start()
        try {
        logger.info("in TestCRUD, update_PartyContactInfo, pci(1): ${pci}")
        pci.create()
        logger.info("in TestCRUD, update_PartyContactInfo, pci(1b): ${pci}")
        String rndStr = String.format("%05d", RANDOM.nextInt(10000))
        updatedEmail = "updatedUser_${rndStr}@test.com"
        logger.info("in TestCRUD, update_PartyContactInfo, updatedEmail(2): ${updatedEmail}")
        pci.set("emailAddress", updatedEmail)
        logger.info("in TestCRUD, update_PartyContactInfo, pci(3): ${pci}")
        pci = pci.update()
        logger.info("in TestCRUD, update_PartyContactInfo, pci (4): ${pci}")

        id = pci.getVertex().id()
        Map m = g.V(id).valueMap('emailAddress').next()
        propEmail = m.emailAddress[0]?:""
        logger.info("in TestCRUD, update_PartyContactInfo, updatedEmail: ${updatedEmail}")
        logger.info("in TestCRUD, update_PartyContactInfo, updatedEmail == propEmail: ${updatedEmail == propEmail}")
        } catch (Exception e) {
            logger.info("in TestCRUD, create_PartyContactInfo, exception: ${e.getMessage()}")
        }

        then:
        updatedEmail == propEmail

        cleanup:
        g.V(id).drop()
        g.close()

    }

    def "delete_PartyContactInfo"() {
        setup:
        org.apache.tinkerpop.gremlin.structure.Vertex vrtx = null
        org.apache.tinkerpop.gremlin.driver.ResultSet rs
        org.apache.tinkerpop.gremlin.driver.Result r
        boolean dropped
        GraphTraversalSource g = JanusGraphUtils.getTraversalSource (ec) , gts

        when:
        logger.info("in TestCRUD.delete, pci(1): ${pci}")
        pci.create()
        vrtx = pci.getVertex()
        logger.info("in TestCRUD.delete, vrtx(1): ${vrtx}")
        Object id = vrtx?.id()
        logger.info("in TestCRUD.delete, id(1): ${id}")
        pci.delete(null)
        vrtx = pci.getVertex()
        logger.info("in TestCRUD.delete, vrtx(2): ${vrtx}")

        try {
            dropped = true
            org.apache.tinkerpop.gremlin.structure.Vertex v = g.V(id).next()
            if (v) {
                dropped = false
            }
            logger.info("in testCreate.groovy delete, dropped: ${dropped}")
        } catch (java.util.NoSuchElementException e) {
            logger.info("in TestCRUD.groovy delete, 'NoSuchElementException: ${e.getMessage()}")
        }

        then:
        dropped
        g.close()

    }

}
