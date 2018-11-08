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


    static org.apache.tinkerpop.gremlin.structure.Vertex storeVertex(org.apache.tinkerpop.gremlin.structure.Vertex passedVertex,
                                                                 String label, Map<String,Object> vertexProperties,
                                                                 org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource  passedTraversalSource,
                                                                 ExecutionContextImpl ec) {

        logger.info("in JGEntityValue::create, passedVertex: ${passedVertex}")
        org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource g = passedTraversalSource
        if (!g) {
            g = getTraversalSource(ec)
        }
        org.apache.tinkerpop.gremlin.structure.Vertex v
        GraphTraversal gts
        if (passedVertex) {
            v = (org.apache.tinkerpop.gremlin.structure.Vertex) JanusGraphUtils.updateProperties(passedVertex, vertexProperties)
        } else {
            gts = g.addV(label)
            v = (org.apache.tinkerpop.gremlin.structure.Vertex) JanusGraphUtils.addProperties(gts, vertexProperties)
        }
//        gts = JanusGraphUtils.addProperties(gts, vertexProperties)
//        org.apache.tinkerpop.gremlin.structure.Vertex v2 = gts.next()
//        logger.info("in JGEntityValue::create, v2: ${v2}")
//        logger.info("in storeVertexAndEdge, (1) e: ${e}")
//        Property prop
//        Object value
//        Map <String,Object> newProps = new HashMap<>()
//        def existingVal
//        // smartly add in edge properties
//        edgeProperties.each {fieldName, val ->
//            logger.info("in JGEntityValue::update, fieldName: ${fieldName}")
//            existingVal = e.property(fieldName)
//            logger.info("in JGEntityValue::update, existingVal: ${existingVal}")
//            val = _this.get(fieldName)
//            logger.info("in JGEntityValue::update,  val: ${val}")
//            if (existingVal != val) {
//                if (existingVal.isPresent()) {
//                    gts.properties(fieldName).drop()
//                }
//                if (val) {
//                    newProps[fieldName] = val
//                }
//                logger.info("fieldName: ${fieldName}, val: ${val}")
//            }
//        }
//        gts.next()
//        // After properties are deleted, add back values
//        newProps.each {fieldName, val ->
//            e.property(fieldName, val )
//        }

        logger.info("in JanusGraphUtils.storeVertex, v: ${v}")
        if (!passedTraversalSource) {
            g.tx().commit()
            g.close()
        }
        return v ? v : null
    }

    static org.apache.tinkerpop.gremlin.structure.Edge storeEdge(org.apache.tinkerpop.gremlin.structure.Vertex fromVertex,
           org.apache.tinkerpop.gremlin.structure.Vertex toVertex,
           String label, Map<String,Object> edgeProperties,
           org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource  passedTraversalSource,
           ExecutionContextImpl ec) {

        org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource g = passedTraversalSource
        if (!g) {
            g = getTraversalSource(ec)
        }
        org.apache.tinkerpop.gremlin.structure.Edge e
        List existingEdges = g.V(fromVertex.id()).outE(label).where(__.otherV().hasId(toVertex.id())).toList()
        logger.info("in storeEdge, existingEdges: ${existingEdges}")
        if( existingEdges) {
            e = existingEdges[0]
            e = (org.apache.tinkerpop.gremlin.structure.Edge) JanusGraphUtils.updateProperties(e, edgeProperties)
        } else {
            GraphTraversal gts = g.V(fromVertex.id()).as('a').V(toVertex.id()).addE(label).from('a')
            e = (org.apache.tinkerpop.gremlin.structure.Edge) JanusGraphUtils.addProperties(gts, edgeProperties)

        }

        logger.info("in JanusGraphUtils.storeEdge, e: ${e}")
        if (!passedTraversalSource) {
            g.tx().commit()
            g.close()
        }
        return e ? e : null
    }

    static Map<String,Object> storeVertexAndEdge(org.apache.tinkerpop.gremlin.structure.Vertex fromVertex,
            EntityValue targetEntity,
            String edgeLabel, Map <String,Object> edgeProperties, Map <String,Object> vertexProperties,
            org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource  passedTraversalSource,
            ExecutionContextImpl ec) throws EntityException {
        logger.info("in storeVertexAndEdge, edgeProperties: ${edgeProperties}")
        logger.info("in storeVertexAndEdge, vertexProperties: ${vertexProperties}")
        EntityFacadeImpl ef = ec.getEntityFacade()
        logger.info("in storeVertexAndEdge, ef: ${ef}")
        String vertexLabel = vertexProperties.get('label')?: fromVertex.label()
        logger.info("in storeVertexAndEdge, vertexLabel: ${vertexLabel}")
        EntityDefinition ed = ef.getEntityDefinition(vertexLabel)

        org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource g = passedTraversalSource
        if (!g) {
            g = getTraversalSource(ec)
        }
        // Add or update vertex
        EntityValue pci = targetEntity
        logger.info("in storeVertexAndEdge, pci: ${pci}")
        //try {
            if (!pci) {
                pci =  new JanusGraphEntityValue(ed, ef, null)
                logger.info("in storeVertexAndEdge, pci (2): ${pci}")
                pci.setAll(vertexProperties)
                logger.info("in storeVertexAndEdge, valueMap: ${pci.getValueMap()}")
                pci.create(g)
            } else {
                pci.setAll(vertexProperties)
                pci.update(g)
            }

        org.apache.tinkerpop.gremlin.structure.Vertex v = pci.getVertex()
        //} catch(EntityException err) {
            //logger.info("in storeVertexAndEdge, err: ${err}")
        //}
        logger.info("in storeVertexAndEdge, pci: ${pci}")
        String thisEdgeLabel = edgeLabel?: edgeProperties._label
        logger.info("in storeVertexAndEdge, thisEdgeLabel: ${thisEdgeLabel}")
        logger.info("in storeVertexAndEdge, fromVertex: ${fromVertex}")
        logger.info("in storeVertexAndEdge, pci.getVertex: ${pci.getVertex()}")
        org.apache.tinkerpop.gremlin.structure.Edge e = JanusGraphUtils.storeEdge(fromVertex, pci.getVertex(),
                                     thisEdgeLabel?:"defaultEdgeLabel", edgeProperties, g, ec)

        if (!passedTraversalSource) {
            g.tx().commit()
            g.close()
        }

        return ['edge':e, 'entity':pci, 'vertex':v]
    }

    static void debugStart (fromName) {
        return
    }

    static org.apache.tinkerpop.gremlin.structure.Vertex getRootVertex(  ExecutionContext ec) {

//        org.apache.tinkerpop.gremlin.structure.Vertex v
//        EntityFacade efi = ec.getEntityFacade()
//        JanusGraphDatasourceFactory ddf = efi.getDatasourceFactory("transactional_nosql") as JanusGraphDatasourceFactory
//        Graph graph = ddf.getDatabase()
        org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource g = getTraversalSource(ec)
        org.apache.tinkerpop.gremlin.structure.Vertex v
        if ( g.V().hasLabel('root').hasNext()) {
            v = g.V().hasLabel('root').next()
        } else {
            v = g.addV('root').next()
        }
        g.tx().commit()
        g.close()
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

    static GraphTraversalSource getTraversalSource (ExecutionContext ec) {
        EntityFacadeImpl efi = ec.getEntityFacade()
        //logger.info("JanusGraphUtils::getTraversalSource, efi: ${efi}")
        return JanusGraphUtils.getTraversalSource(efi)
    }

    static GraphTraversalSource getTraversalSource (EntityFacadeImpl efi) {
        JanusGraphDatasourceFactory edfi = efi.getDatasourceFactory("transactional_nosql") as JanusGraphDatasourceFactory
        //logger.info("JanusGraphUtils::getTraversalSource, edfi: ${edfi}")
        //Graph jG = JanusGraphUtils.getDatabase(ec)
        GraphTraversalSource g = edfi.getTraversalSource()
        //logger.info("JanusGraphUtils::getTraversalSource, g: ${g}")
        return g
    }

    static org.apache.tinkerpop.gremlin.structure.Element updateProperties(org.apache.tinkerpop.gremlin.structure.Element elem, Map<String,Object> props) {
        Object elemVal
        props.each {k,v ->
            elemVal = elem.property(k).value()
            if (elemVal) {
                if (elemVal == v) {
                } else {
                    if (v) {
                        elem.property(k, v)
                    } else {
                        elem.remove()
                    }
                }
            } else {
                if (v) {
                    elem.property(k, v)
                }
            }
        }
        java.util.Date sameDate = new java.util.Date(new Timestamp(System.currentTimeMillis()).getTime())
        long sameTime = sameDate.getTime()
        elem.property('lastUpdatedStamp', sameTime)
        return elem
    }

    static org.apache.tinkerpop.gremlin.structure.Element addProperties(GraphTraversal gts, Map<String,Object> props) {

        props.each { fieldName, val ->
            logger.info("in JanusGraphUtils.addProperties, fieldName: ${fieldName}, val: ${val}")
            if (val) {
                gts = gts.property(fieldName, val)
            }
        }
        java.util.Date sameDate = new java.util.Date(new Timestamp(System.currentTimeMillis()).getTime())
        long sameTime = sameDate.getTime()
        gts = gts.property('createdDate', sameTime)
        gts = gts.property('lastUpdatedStamp', sameTime)
        org.apache.tinkerpop.gremlin.structure.Element elem = gts.next()
        return elem
    }
}
