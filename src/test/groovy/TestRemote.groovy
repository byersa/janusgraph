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
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Direction
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper
import org.apache.tinkerpop.gremlin.driver.Client

import org.moqui.addons.graph.JanusGraphUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class TestRemote extends Specification {
    protected final static Logger logger = LoggerFactory.getLogger(Moqui.class)

    @Shared
    ExecutionContext ec

    @Shared
    Graph graph

    @Shared
    Timestamp nowTimestamp

    org.apache.tinkerpop.gremlin.driver.Client client

    def setupSpec() {
        // init the framework, get the ec
        logger.info("in TestRemote, setupSpec")
        ec = Moqui.getExecutionContext()
        logger.info("in TestRemote, ec: ${ec}")
        graph = JanusGraphUtils.getDatabase(ec)
        logger.info("in TestRemote, graph: ${graph}")
        nowTimestamp = ec.user.nowTimestamp
    }

    def cleanupSpec() {
        //janusGraph.close()
        ec.destroy()
    }

    def setup() {
        client = JanusGraphUtils.getClient(ec)
        return
    }

    def cleanup() {
        client.close()
        return
    }

    def "test_remote"() {
        when:
        //org.apache.tinkerpop.gremlin.driver.Cluster.Builder builder = org.apache.tinkerpop.gremlin.driver.Cluster.build()
        //GryoMapper mapper = GryoMapper.build().addRegistry(JanusGraphIoRegistry.INSTANCE).create()
        //builder.addContactPoint("192.168.1.197")
        //builder.port(8182)
        //builder.serializer(new org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0(mapper))
        //org.apache.tinkerpop.gremlin.driver.Cluster cluster = builder.create()
        //org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource g=org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph.instance().traversal().withRemote(org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection.using(cluster, "g"))
        GraphTraversalSource g = JanusGraphUtils.getTraversal(ec)
        logger.info("test_remote, g: ${g}")
        org.apache.tinkerpop.gremlin.structure.Vertex v2 = g.addV('test').next()
        logger.info("test_remote, v2: ${v2}")


//        String gremlin = "g.addV('testV3').next()"
//        logger.info("test_remote, (1) gremlin: ${gremlin}")
//        org.apache.tinkerpop.gremlin.driver.ResultSet results = client.submit(gremlin)
//        org.apache.tinkerpop.gremlin.structure.Vertex v = results.one().getVertex()
//        logger.info("test_remote, v: ${v}")
//        Object id = v.id()
//        logger.info("test_remote, v.id: ${id}")
//        gremlin = "g.V().hasId('${id}')"
//        logger.info("test_remote, (2) gremlin: ${gremlin}")
//        org.apache.tinkerpop.gremlin.driver.ResultSet results2 = client.submit(gremlin)
//        org.apache.tinkerpop.gremlin.structure.Vertex v2 = results2.one().getVertex()
//        logger.info("test_remote, v2: ${v2}")

        then:
        assert(v2)

        cleanup:
        g.V('${v2.id()}').drop()
//        logger.info("test_remote, (3) gremlin: ${gremlin}")
//        org.apache.tinkerpop.gremlin.driver.ResultSet results3 = client.submit(gremlin)
    }

}
