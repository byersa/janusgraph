/*
 * This Work is in the public domain and is provided on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied,
 * including, without limitation, any warranties or conditions of TITLE,
 * NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE.
 * You are solely responsible for determining the appropriateness of using
 * this Work and assume any risks associated with your use of this Work.
 *
 * This Work includes contributions authored by Al Byers, not as a
 * "work for hire", who hereby disclaims any copyright to the same.
 */
package org.moqui.addons.janusgraph

import org.apache.tinkerpop.gremlin.structure.Edge
import org.janusgraph.core.Multiplicity
import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.moqui.addons.janusgraph.JanusGraphEntityValue
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.entity.EntityFind
import org.moqui.impl.entity.EntityFindImpl
import org.moqui.entity.EntityValue
import org.moqui.impl.entity.EntityValueImpl
import org.apache.tinkerpop.gremlin.driver.Client
import org.moqui.resource.ResourceReference
//import org.apache.commons.configuration2.Configuration
//import org.apache.tinkerpop.gremlin.util.config.YamlConfiguration

import org.joda.time.format.*

import javax.sql.DataSource

import org.moqui.entity.*

import java.sql.Timestamp
import java.sql.Types

import java.util.HashMap
import java.util.Map
import java.lang.Exception

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.moqui.util.MNode

import org.janusgraph.core.JanusGraphFactory
import org.janusgraph.core.JanusGraph
import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.core.VertexLabel
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.EdgeLabel
import org.janusgraph.graphdb.tinkerpop.JanusGraphIoRegistry
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__

import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper
import org.apache.tinkerpop.gremlin.structure.io.gryo.GryoMapper.Builder
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.apache.tinkerpop.gremlin.driver.Cluster
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection
import org.apache.tinkerpop.gremlin.structure.Graph

//import org.apache.commons.configuration2.YAMLConfiguration

/**
 * To use this:
 * 1. add a datasource under the entity-facade element in the Moqui Conf file; for example:
 *      <datasource group-name="transactional_nosql" object-factory="org.moqui.impl.entity.janusgraph.JanusGraphDatasourceFactory">
 *          <inline-other uri="local:runtime/db/orient/transactional" username="moqui" password="moqui"/>
 *      </datasource>
 *
 * 3. add the group-name attribute to entity elements as needed to point them to the new datasource; for example:
 *      group-name="transactional_nosql"
 */

class JanusGraphDatasourceFactory implements EntityDatasourceFactory {
    protected final static Logger logger = LoggerFactory.getLogger(JanusGraphDatasourceFactory.class)

    protected EntityFacadeImpl efi
    protected MNode datasourceNode
    protected String tenantId

    protected Graph graph
    protected Graph janusGraph
    protected GraphTraversalSource janusGraphClient
    protected org.apache.tinkerpop.gremlin.driver.Cluster cluster
    protected String contactPoint = "192.168.1.197" //"localhost"
    protected int port = 8182
    //protected GryoMapper.Builder mapperBuilder = GryoMapper.build()
    protected GryoMapper mapper //= mapperBuilder.addRegistry(JanusGraphIoRegistry.INSTANCE).create()

    protected serializer //= new org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0(mapper)
    //protected JanusGraphManagement janusGraphMgmt


    JanusGraphDatasourceFactory() { 
    }

    EntityDatasourceFactory init(org.moqui.entity.EntityFacade ef, org.moqui.util.MNode nd) {
        // local fields
        this.efi = (EntityFacadeImpl) ef
        this.datasourceNode = nd

        logger.info("JanusGraphDatasourceFactory, JanusGraphIoRegistry(1): ${JanusGraphIoRegistry}")
        JanusGraphIoRegistry registry = JanusGraphIoRegistry.getInstance()
        logger.info("JanusGraphDatasourceFactory, registry (1): ${registry}")
        JanusGraphIoRegistry registry2 = JanusGraphIoRegistry.INSTANCE
        logger.info("JanusGraphDatasourceFactory, registry2 (2): ${registry2}")
        GryoMapper.Builder mapperBuilder = GryoMapper.build().addRegistry(registry)
//        GryoMapper mapper = mapperBuilder.create()
        serializer = new org.apache.tinkerpop.gremlin.driver.ser.GryoMessageSerializerV3d0(mapperBuilder)
//        Cluster.Builder builder = Cluster.build()
//        builder.addContactPoint(contactPoint)
//        builder.port(port)
//        builder.serializer(serializer)
//        cluster = builder.create()
        cluster = Cluster.build().
                addContactPoint(contactPoint).
                port(port).
                serializer(serializer).
                create()

        janusGraph = JanusGraphFactory.build()
                .set("storage.backend", "cql")
                .set("storage.hostname", contactPoint)
                .set("index.search.backend", "elasticsearch")
                .set("index.search.hostname", contactPoint)
                     .open()
        graph = EmptyGraph.instance()
        //janusGraphClient = janusGraph.traversal()
        //janusGraphMgmt = janusGraph.openManagement()
        //logger.info("janusGraphClient: ${janusGraphClient}")
        initCreateAndUpdateStamps()
        return this
    }

    GraphTraversalSource getTraversal() {
        GraphTraversalSource g = janusGraph.traversal()
        //GraphTraversalSource g = graph.traversal().withRemote(DriverRemoteConnection.using(cluster, "g"))
        //ResourceReference remoteRef = this.efi.ecfi.resourceFacade.getLocationReference("component://graph/conf/remote-graph.properties")
        //InputStream inp = remoteRef.openStream()
        //org.apache.tinkerpop.gremlin.util.config.YamlConfiguration yamlConfig = new org.apache.tinkerpop.gremlin.util.config.YamlConfiguration()
        //yamlConfig.load(new InputStreamReader(inp))
        //String txt = inp.text
        //logger.info("JanusGraphDatasourceFactory::getTraversal, txt: ${txt}")
        //inp.close()
        //inp = remoteRef.openStream()
        //Configuration config = new YAMLConfiguration().read(inp)
        //GraphTraversalSource g = graph.traversal().withRemote(yamlConfig)
        //logger.info("JanusGraphDatasourceFactory::getTraversal, g: ${g}")
        return g
    }

    org.apache.tinkerpop.gremlin.driver.Client getClient() {
        org.apache.tinkerpop.gremlin.driver.Client client = cluster.connect().init()
        return client
    }

    void initCreateAndUpdateStamps() {

//        logger.info("in initCreateAndUpdateStamp")
//        PropertyKey propKey
//        ManagementSystem mgmt = janusGraph.openManagement()
//        List <String> fieldNames = ["createdDate", "lastUpdatedStamp"]
//        fieldNames.each() { fieldName ->
//            propKey = mgmt.getPropertyKey(fieldName)
//            logger.info("propKey (1): ${propKey}")
//            if (!propKey) {
//                logger.info("fieldName: ${fieldName}")
//                mgmt.makePropertyKey(fieldName).dataType(java.util.Date.class).make()
//                propKey = mgmt.getPropertyKey(fieldName)
//                logger.info("propKey (2): ${propKey.name()}")
//            } else {
//                logger.info("existing propKey: ${fieldName}")
//            }
//        }
        return
    }

    GraphTraversalSource getJanusGraphClient() { return janusGraphClient}

    /** Returns the main database access object for OrientDB.
     * Remember to call close() on it when you're done with it (preferably in a try/finally block)!
     */
    Graph getDatabase() {
        return graph
    }

    String getGroupName() { return datasourceNode.attribute("group-name")}

    @Override
    EntityValue makeEntityValue(String entityName) {
        EntityDefinition entityDefinition = efi.getEntityDefinition(entityName)
        if (!entityDefinition) {
            throw new EntityException("Entity not found for name [${entityName}]")
        }
        JanusGraphEntityValue entityValue = new JanusGraphEntityValue(entityDefinition, efi, this)
        return entityValue
    }

    @Override
    EntityFind makeEntityFind(String entityName) {
        return new JanusGraphEntityFind(efi, entityName, this)
    }
    @Override
    DataSource getDataSource() { return null }

    @Override
    void checkAndAddTable(java.lang.String tableName) {


//            logger.info("checking: ${tableName}")
//        janusGraph.tx().rollback()
//        JanusGraphManagement mgmt = janusGraph.openManagement()
//        //JanusGraphManagement mgmt = janusGraph.openManagement()
//        logger.info("checking isOpen(1): ${mgmt.isOpen()}")
//
//            String labelString
//            def ed = efi.getEntityDefinition(tableName)
//            String entityName = ed.getEntityName()
//            MNode entityNode = ed.getEntityNode()
//            String graphMode = entityNode.attribute("graphMode")
//        Character edgeOrVertex = Character.toLowerCase(entityName.charAt(0))
//        logger.info("edgeOrVertex: ${edgeOrVertex}")
//        if (edgeOrVertex == 'e') {
//                EdgeLabel edgeLabel = mgmt.getOrCreateEdgeLabel(entityName)
////                if (!edgeLabel) {
////                    mgmt.makeEdgeLabel(entityName).multiplicity(Multiplicity.MULTI).make()
////                }
////                edgeLabel = mgmt.getEdgeLabel(entityName)
//                if (!edgeLabel || (edgeLabel.name() != entityName)) {
//                    throw new Exception( "Cannot create vertex label: " + entityName)
//                }
//        }
//        if (edgeOrVertex == 'v') {
//            VertexLabel vertexLabel = mgmt.getOrCreateVertexLabel(entityName)
////            VertexLabel vertexLabel = mgmt.getVertexLabel(entityName)
////                if (!vertexLabel) {
////                    mgmt.makeVertexLabel(entityName).make()
////                }
////                vertexLabel = mgmt.getVertexLabel(entityName)
//                if (!vertexLabel || (vertexLabel.name() != entityName)) {
//                    throw new Exception( "Cannot create vertex label: " + entityName)
//                }
//        }
//
//        if (edgeOrVertex == 'e' || edgeOrVertex == 'v') {
//            List<String> fieldNames = ed.getFieldNames(true, true)
//            MNode nd
//            String typ
//            Class clazz
//            PropertyKey propKey
//            fieldNames.each() { fieldName ->
//                propKey = mgmt.getPropertyKey(fieldName)
//                if (!propKey) {
//                    nd = ed.getFieldNode(fieldName)
//                    typ = nd.attribute("type")
//                    clazz = getDataType(typ)
//                    logger.info("fieldName: ${fieldName}")
//                    mgmt.makePropertyKey(fieldName).dataType(clazz).make()
//                    propKey = mgmt.getPropertyKey(fieldName)
//                    logger.info("propKey: ${propKey.name()}")
//                } else {
//                    logger.info("existing propKey: ${propKey.name()}")
//                }
//            }
//        } else {
//            logger.info("NOT processing: ${entityName}")
//        }
//        Iterator <VertexLabel> iter = mgmt.getVertexLabels().iterator()
//        VertexLabel lbl
//        while (iter.hasNext()) {
//           lbl = iter.next()
//            logger.info("vertex label: ${lbl.name()}")
//        }
//        mgmt.commit()
        return
    }

    void createTable(tableName) {
//        CreateTableRequest request = this.getCreateTableRequest(tableName)
//        CreateTableResult createTableResult = janusGraphClient.createTable(request)
//                    logger.info("isActive: ${createTableResult.getTableDescription()}")
    } 


    Class getDataType(fieldType) {

                   Class dataTypeClass
                   switch(fieldType) {
                        case "id":
                        case "id-long":
                        case "text-short":
                        case "text-medium":
                        case "text-long":
                        case "text-very-long":
                        case "text-indicator":
                            dataTypeClass = (Class)String.class
                            break
                        case "number-integer":
                        case "number-decimal":
                        case "number-float":
                        case "currency-amount":
                        case "currency-precise":
                        case "time":
                            dataTypeClass = (Class)Double.class
                            break
                        case "date":
                        case "date-time":
                             dataTypeClass = (Class)java.util.Date.class
                             break
                        default:
                             dataTypeClass = (Class)String.class
                   }
                   return dataTypeClass
    }
    // Dummied out methods
    boolean checkTableExists(java.lang.String s) {return null}

    void destroy() {
        super.destroy()
        graph.close()
        return
    }
}
