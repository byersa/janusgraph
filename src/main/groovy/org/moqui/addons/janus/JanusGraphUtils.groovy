package org.moqui.addons.janusgraph

import org.janusgraph.graphdb.database.StandardJanusGraph
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.Property
import org.apache.tinkerpop.gremlin.structure.Element
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.ResultSet

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__

import java.sql.Timestamp
import java.util.concurrent.CompletableFuture
import org.moqui.context.ExecutionContext
import org.moqui.impl.context.ExecutionContextImpl
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.entity.EntityFacade
import org.moqui.entity.EntityValue
import org.moqui.impl.entity.EntityValueImpl
import org.moqui.entity.EntityDatasourceFactory
import org.moqui.addons.janusgraph.JanusGraphEntityValue
import org.moqui.entity.EntityCondition
import org.moqui.entity.EntityException

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.moqui.util.MNode 

class JanusGraphUtils {

    protected final static Logger logger = LoggerFactory.getLogger(JanusGraphUtils.class)

    static Object getAttributeValue(String fieldName, Map<String,?>valueMap, EntityDefinition ed) {
    
        Object attrVal = new Object()
        MNode fieldNode = ed.getFieldNode(fieldName)
        String fieldNodeName = fieldNode."@name"
                 logger.info("JanusGraphUtils.getAttributeValue(291) fieldNodeName: ${fieldNodeName}")
        String fieldNodeType = fieldNode."@type"
                 logger.info("JanusGraphUtils.getAttributeValue(293) fieldNodeType: ${fieldNodeType}")
        switch(fieldNodeType) {
            case "id":
            case "id-long":
            case "text-short":
            case "text-medium":
            case "text-long":
            case "text-very-long":
            case "text-indicator":
                 String val = valueMap.get(fieldName)?: ""
                 logger.info("JanusGraphUtils.getAttributeValue(32) val: ${val}")
                 if (val) {
                    attrVal.setS(val)
                 } else {
                     return null
                 }
                 break
            case "number-integer":
                 String val = valueMap.get(fieldName)?: ""
                 logger.info("JanusGraphUtils.getAttributeValue(41) val: ${val}")
                 if (val) {
                    attrVal.setN(val)
                 } else {
                     return null
                 }
                 break
            case "number-decimal":
            case "number-float":
            case "currency-amount":
            case "currency-precise":
                 return attrVal.setN(valueMap.get(fieldName).toString())
                 break
            case "date":
            case "time":
            case "date-time":
                 String dateTimeStr = valueMap.get(fieldName)
                 logger.info("JanusGraphUtils(310) dateTimeStr: ${dateTimeStr}")
                 if (dateTimeStr != null) {
                     Timestamp ts = Timestamp.valueOf(dateTimeStr)
                     logger.info("JanusGraphUtils(312) ts: ${ts.toString()}")
                     attrVal.setS(ts.toString())
                 } else {
                     return null
                 }
                 break
            default:
                 String val = valueMap.get(fieldName)?: ""
                 if (val) {
                     attrVal.setS(val)
                 } else {
                     return null
                 }
        }
        
        return attrVal
    }
    
    static Object getAttributeValueUpdate(String fieldName, Map<String,?>valueMap, EntityDefinition ed) {
    
        Object attrVal = new Object()
        MNode fieldNode = ed.getFieldNode(fieldName)
        String fieldNodeName = fieldNode."@name"
                 logger.info("JanusGraphUtils.getAttributeValue(291) fieldNodeName: ${fieldNodeName}")
        String fieldNodeType = fieldNode."@type"
                 logger.info("JanusGraphUtils.getAttributeValue(293) fieldNodeType: ${fieldNodeType}")
                 
        // do not include hash key field
        if (fieldNodeName in ed.getPkFieldNames()) {
             return null
        }
        // do not include range key field
        String indexName = fieldNode."@index"
        if (indexName) {
                return null
        }
        switch(fieldNodeType) {
            case "id":
            case "id-long":
            case "text-short":
            case "text-medium":
            case "text-long":
            case "text-very-long":
            case "text-indicator":
                 String val = valueMap.get(fieldName)?: ""
                 logger.info("JanusGraphUtils.getAttributeValue(32) val: ${val}")
                 if (val) {
                    attrVal.setValue(new Object().withS(val))
                 } else {
                     return null
                 }
                 break
            case "number-integer":
                 String val = valueMap.get(fieldName)?: ""
                 logger.info("JanusGraphUtils.getAttributeValue(41) val: ${val}")
                 if (val) {
                    attrVal.setValue(new Object().withN(val))
                 } else {
                     return null
                 }
                 break
            case "number-decimal":
            case "number-float":
            case "currency-amount":
            case "currency-precise":
                 return attrVal.setN(valueMap.get(fieldName).toString())
                 break
            case "date":
            case "time":
            case "date-time":
                 String dateTimeStr = valueMap.get(fieldName)
                 logger.info("JanusGraphUtils(310) dateTimeStr: ${dateTimeStr}")
                 Timestamp ts = Timestamp.valueOf(dateTimeStr)
                 logger.info("JanusGraphUtils(312) ts: ${ts.toString()}")
                 attrVal.setValue(new Object().withS(ts.toString()))
                 break
            default:
                 String val = valueMap.get(fieldName)?: ""
                 if (val) {
                    attrVal.setValue(new Object().withS(val))
                 } else {
                     return null
                 }
        }
        
        return attrVal
    }


    static org.apache.tinkerpop.gremlin.structure.Edge addEdge(org.apache.tinkerpop.gremlin.structure.Vertex fromVertex,
                        org.apache.tinkerpop.gremlin.structure.Vertex toVertex,
                        String label, Map<String,Object> edgeProperties,
                        org.apache.tinkerpop.gremlin.driver.Client passedClient,
                                                               ExecutionContextImpl ec) {

        org.apache.tinkerpop.gremlin.driver.Client client = passedClient
        if (!client) {
            client = JanusGraphUtils.getClient(ec)
        }
        logger.info("in addEdge fromVertex id: ${fromVertex.id()}")
        Object fromId = fromVertex.id()
        Object toId = toVertex.id()
        StringBuilder eStr = StringBuilder.newInstance()
        eStr << "g.V('${fromId}').as('a').V('${toId}').addE('${label}').from('a')"
        Property prop
        Object value
        edgeProperties.each {fieldName, val ->
            eStr << ".property('${fieldName}', "
            if (val instanceof String) {
                eStr << "'${val}'"
            } else {
                eStr << "${val}"
            }
            eStr << ")"
        }
        eStr << ".next()"
        logger.info("in addEdge eStr: ${eStr.toString()}")
        org.apache.tinkerpop.gremlin.driver.ResultSet results = client.submit(eStr.toString());
        Edge edge = results.one().getEdge();

        logger.info("Edge: ${edge}")
        if (!passedClient) {
            client.close()
        }
        return edge ? edge : null
    }

    static EntityValue storeVertexAndEdge(org.apache.tinkerpop.gremlin.structure.Vertex fromVertex,
            EntityValue targetEntity,
            String edgeLabel, Map <String,Object> edgeProperties, Map <String,Object> vertexProperties,
                                          org.apache.tinkerpop.gremlin.driver.Client passedClient,
                                          ExecutionContextImpl ec) throws EntityException {
        EntityFacadeImpl ef = ec.getEntityFacade()
        String vertexLabel = vertexProperties.get('label')?: fromVertex.label()
        logger.info("in storeVertexAndEdge, vertexLabel: ${vertexLabel}")
        EntityDefinition ed = ef.getEntityDefinition(vertexLabel)

        org.apache.tinkerpop.gremlin.driver.Client client = passedClient
        if (!client) {
            client = JanusGraphUtils.getClient(ec)
        }
        // Add or update "to" vertex
        Object thisId = vertexProperties?.id

        EntityValue pci = targetEntity
        //try {
            if (!pci) {
                pci =  new JanusGraphEntityValue(ed, ef, null)
                pci.setAll(vertexProperties)
                pci.create(client)
            } else {
                pci.setAll(vertexProperties)
                pci.update(client)
            }
        //} catch(EntityException err) {
            //logger.info("in storeVertexAndEdge, err: ${err}")
        //}
        logger.info("in storeVertexAndEdge, pci: ${pci}")
        String thisEdgeLabel = edgeLabel?: edgeProperties.label
        logger.info("in storeVertexAndEdge, thisEdgeLabel: ${thisEdgeLabel}")
        org.apache.tinkerpop.gremlin.structure.Edge e
        if (targetEntity) {
            String gremlin = "g.V('${fromVertex.id()}').outE('${thisEdgeLabel}').where(__.otherV().hasId('${targetEntity.getVertex().id()}')).iterator()"
            logger.info("in storeVertexAndEdge, gremlin: ${gremlin}")
            org.apache.tinkerpop.gremlin.driver.ResultSet results = client.submit(gremlin.toString());
            e = results.one()?.getEdge();
            logger.info("in storeVertexAndEdge, (1) e: ${e}")
        }
        if (!e) {
            e = JanusGraphUtils.addEdge(fromVertex, pci.getVertex(), thisEdgeLabel?:"defaultEdgeLabel", edgeProperties, client, ec)
            logger.info("in storeVertexAndEdge, (2) e: ${e}")
        }

        // smartly add in edge properties
        //JanusGraphUtils.addProperties(e, edgeProperties)

        if (!passedClient) {
            client.close()
        }
        return pci
    }

    static void debugStart (fromName) {
        return
    }

    static org.apache.tinkerpop.gremlin.structure.Vertex getRootVertex(  ExecutionContext ec) {

//        org.apache.tinkerpop.gremlin.structure.Vertex v
//        EntityFacade efi = ec.getEntityFacade()
//        JanusGraphDatasourceFactory ddf = efi.getDatasourceFactory("transactional_nosql") as JanusGraphDatasourceFactory
//        Graph graph = ddf.getDatabase()
        org.apache.tinkerpop.gremlin.driver.Client client = JanusGraphUtils.getClient(ec)
        String gremlin = "g.V().hasLabel('root').next()"
        org.apache.tinkerpop.gremlin.driver.ResultSet results = client.submit(gremlin.toString());
        org.apache.tinkerpop.gremlin.structure.Vertex v = results.one()?.getVertex();
        if (!v) {
            gremlin = "g.addV('root').next()"
            results = client.submit(gremlin.toString());
            v = results.one()?.getVertex();
        }
        client.close()
        return v
    }

    static Graph instantiateDatabase(ExecutionContextImpl ec) {
        return JanusGraphUtils.getDatabase(ec)
    }

    static Graph getDatabase(ExecutionContextImpl ec) {
        EntityFacadeImpl efi = ec.getEntityFacade()
        JanusGraphDatasourceFactory edfi = efi.getDatasourceFactory("transactional_nosql") as JanusGraphDatasourceFactory
        Graph jG = edfi.getDatabase()
        return jG
    }

    static  org.apache.tinkerpop.gremlin.driver.Client getClient (ExecutionContext ec) {
        EntityFacadeImpl efi = ec.getEntityFacade()
        logger.info("JanusGraphUtils::getClient, efi: ${efi}")
        JanusGraphDatasourceFactory edfi = efi.getDatasourceFactory("transactional_nosql") as JanusGraphDatasourceFactory
        logger.info("JanusGraphUtils::getClient, edfi: ${edfi}")
        org.apache.tinkerpop.gremlin.driver.Client client = edfi.getClient()
        logger.info("JanusGraphUtils::getClient, client: ${client}")
        return client
    }

    static GraphTraversalSource getTraversal (ExecutionContext ec) {
        EntityFacadeImpl efi = ec.getEntityFacade()
        logger.info("JanusGraphUtils::getTraversal, efi: ${efi}")
        JanusGraphDatasourceFactory edfi = efi.getDatasourceFactory("transactional_nosql") as JanusGraphDatasourceFactory
        logger.info("JanusGraphUtils::getTraversal, edfi: ${edfi}")
        //Graph jG = JanusGraphUtils.getDatabase(ec)
        GraphTraversalSource g = edfi.getTraversal()
        logger.info("JanusGraphUtils::getTraversal, g: ${g}")
        return g
    }

    static void addProperties(org.apache.tinkerpop.gremlin.structure.Element elem, Map<String,Object> props) {
        Object elemVal
        props.each {k,v ->
            elemVal = elem.property(k)
            if (elemVal == v) {
            } else {
                elem.property(k,v)
            }
        }
        return
    }
}
