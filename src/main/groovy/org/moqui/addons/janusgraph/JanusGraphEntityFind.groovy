/*
 * This Work is in the public domain and is provided on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied,
 * including, without limitation, any warranties or conditions of TITLE,
 * NON-INFRINGEMENT, MERCHANTABILITY, or FITNESS FOR A PARTICULAR PURPOSE.
 * You are solely responsible for determining the appropriateness of using
 * this Work and assume any risks associated with your use of this Work.
 *
 * This Work includes contributions authored by David E. Jones, not as a
 * "work for hire", who hereby disclaims any copyright to the same.
 */
package org.moqui.addons.janusgraph

import static org.apache.tinkerpop.gremlin.process.traversal.P.*
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.P
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.ResultSet
import org.apache.tinkerpop.gremlin.driver.Result

import java.sql.ResultSet
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.CompletableFuture

import org.moqui.entity.EntityDynamicView

import org.moqui.entity.EntityCondition.JoinOperator
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.impl.entity.EntityValueImpl
import org.moqui.entity.EntityFacade
import org.moqui.entity.EntityListIterator
import org.moqui.entity.EntityException
import org.moqui.entity.EntityCondition
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityListImpl
import org.moqui.impl.entity.EntityFindBuilder
import org.moqui.impl.entity.EntityFindBase
import org.moqui.impl.entity.condition.*
import org.moqui.impl.entity.EntityValueBase
import org.moqui.impl.entity.FieldInfo
import org.moqui.entity.*
import org.moqui.impl.entity.EntityJavaUtil.FieldOrderOptions
import org.moqui.context.ExecutionContext
import org.moqui.impl.context.ExecutionContextImpl

import org.moqui.addons.janusgraph.JanusGraphEntityValue
import org.moqui.addons.janusgraph.JanusGraphDatasourceFactory
import org.moqui.addons.janusgraph.JanusGraphUtils
import org.janusgraph.core.JanusGraph
import org.moqui.impl.context.ExecutionContextFactoryImpl

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.moqui.util.MNode 

class JanusGraphEntityFind { //extends EntityFindBase
    protected final static Logger logger = LoggerFactory.getLogger(JanusGraphEntityFind.class)
    protected JanusGraph janusGraph
    protected JanusGraphDatasourceFactory ddf
    protected EntityFacadeImpl efi
    protected Object fromVertexId
    protected String edgeLabel
    protected String vertexLabel
    protected List <List<Object>> edgeProperties
    protected List <List<Object>> vertexProperties
    protected String groupName = "transactional_nosql"

    org.apache.tinkerpop.gremlin.driver.Client client

    JanusGraphEntityFind(ExecutionContext ec) {
        this.efi = ec.entity
        this.ddf = efi.getDatasourceFactory(groupName) as JanusGraphDatasourceFactory
        this.client = this.ddf.getClient()
    }

    // ======================== Run Find Methods ==============================

    EntityValue one() throws EntityException {

        GraphTraversalSource g = JanusGraphUtils.getTraversalSource(efi)
        GraphTraversal gts
        long startTime = System.currentTimeMillis()
        List retList = null
    try {
        if(fromVertexId) {
            gts = g.V(fromVertexId)
        } else {
            gts = g.V()
        }
        if (vertexProperties && vertexProperties.size()) {
            vertexProperties.each { tuple ->
                gts = applyPredicate(tuple[0], tuple[1], tuple[2], gts)
            }
        }
    } catch (Exception e) {
        logger.error(e.getMessage())
        return null
    }

    try {
        List <org.apache.tinkerpop.gremlin.structure.Vertex> resultList = gts.toList()
        EntityValue entityValue
        EntityDefinition ed
        org.apache.tinkerpop.gremlin.structure.Vertex vtx
        if (resultList.size()) {
            vtx = resultList[0]
            ed = efi.getEntityDefinition(vtx.label())
            // TODO: if no ed then throw exception
            entityValue = new JanusGraphEntityValue(ed, efi, vtx)
        } else {
            entityValue = null
        }
    } catch (Exception e) {
        logger.error(e.getMessage())
        return null
    }
        g.tx().commit()
        g.close()
        return entityValue
    }

    /** @see org.moqui.entity.EntityFind#list() */
    List <EntityValue> list() {

        GraphTraversalSource g = JanusGraphUtils.getTraversalSource(efi)
        GraphTraversal gts

        long startTime = System.currentTimeMillis()
        //EntityDefinition ed = this.getEntityDef()
        List retList = null
        List <org.apache.tinkerpop.gremlin.structure.Vertex> resultList
        //JanusGraphEntityValue entValue = null
        //EntityList entList = new EntityListImpl(this.efi)
        //logger.info("JanusGraphEntityFind.list efi: ${this.efi}")
        try {
            if (fromVertexId) {
                gts = g.V(fromVertexId)
            } else {
                gts = g.V()
            }
            if (edgeLabel) {
                gts = gts.outE(edgeLabel)
            } else {
                gts = gts.outE()
            }
            if (edgeProperties && edgeProperties.size()) {
                edgeProperties.each { tuple ->
                    gts = applyPredicate(tuple[0], tuple[1], tuple[2], gts)
                }
            }
            if (vertexLabel) {
                gts = gts.inV().hasLabel(vertexLabel)
            } else {
                gts = gts.inV()
            }
            if (vertexProperties && vertexProperties.size()) {
                vertexProperties.each { tuple ->
                    gts = applyPredicate(tuple[0], tuple[1], tuple[2], gts)
                }
            }
            resultList = gts.toList()
        } catch (Exception e) {
            logger.error(e.getMessage())
            return null
        }

        EntityValue entityValue
        EntityDefinition ed
        org.apache.tinkerpop.gremlin.structure.Vertex vtx
        List <EntityValue> entityValueList = new ArrayList()
        try {
            resultList.each {v ->
                ed = efi.getEntityDefinition(v.label())
                // TODO: if no ed then throw exception
                entityValue = new JanusGraphEntityValue(ed, efi, v)
                entityValueList << entityValue
            }
        } catch (Exception e) {
            logger.error(e.getMessage())
            return null
        }
        g.tx().commit()
        g.close()
        return entityValueList
    }

    JanusGraphEntityFind condition(Object fromVertexId, String vertexLabel, String edgeLabel, List <List<Object>> edgeProperties, List <List<Object>> vertexProperties) {
        this.fromVertexId = fromVertexId
        this.edgeLabel = edgeLabel
        this.vertexLabel = vertexLabel
        this.edgeProperties = edgeProperties
        this.vertexProperties = vertexProperties
        return this
    }

    EntityFacadeImpl getEntityFacadeImpl() {
        return this.efi
    }

    GraphTraversal applyPredicate( String propName, String predicateText, Object propValue, GraphTraversal gts) {

        GraphTraversal newGts
        Object predicateValue = propValue
//        if (propValue instanceof String) {
//            predicateValue = "'${propValue}'"
//        } else if (propValue instanceof Float) {
//            predicateValue = "${propValue}f"
//        } else {
//            predicateValue = "${propValue}"
//        }

        switch (predicateText) {
            case "eq":
                newGts =  gts.has(propName,  eq(predicateValue))
                break
            case "neq":
                newGts =  gts.has(propName,  neq(predicateValue))
                break
            case "lt":
                newGts =  gts.has(propName,  lt(predicateValue))
                break
            case "lte":
                newGts =  gts.has(propName,  lte(predicateValue))
                break
            case "gt":
                newGts =  gts.has(propName,  gt(predicateValue))
                break
            case "gte":
                newGts =  gts.has(propName,  gte(predicateValue))
                break
            case "inside":
                newGts =  gts.has(propName,  inside(predicateValue))
                break
            case "outside":
                newGts =  gts.has(propName,  outside(predicateValue))
                break
            case "between":
                newGts =  gts.has(propName,  between(predicateValue))
                break
            case "within":
                newGts =  gts.has(propName,  within(predicateValue))
                break
            case "without":
                newGts =  gts.has(propName,  without(predicateValue))
                break
            default:
                newGts =  gts.has(propName,  eq(predicateValue))
                break
        }
        return newGts
    }
//    def applyPredicate( edgeOrVertex, String propName, String predicateText, Object propValue) {
//        switch (predicateText) {
//            case "eq":
//                return edgeOrVertex.has(propName, P.eq(propValue))
//                break
//            case "neq":
//                return edgeOrVertex.has(propName, P.neq(propValue))
//                break
//            case "lt":
//                return edgeOrVertex.has(propName, P.lt(propValue))
//                break
//            case "lte":
//                return edgeOrVertex.has(propName, P.lte(propValue))
//                break
//            case "gt":
//                return edgeOrVertex.has(propName, P.gt(propValue))
//                break
//            case "gte":
//                return edgeOrVertex.has(propName, P.gte(propValue))
//                break
//            case "inside":
//                return edgeOrVertex.has(propName, P.inside(propValue))
//                break
//            case "outside":
//                return edgeOrVertex.has(propName, P.outside(propValue))
//                break
//            case "between":
//                return edgeOrVertex.has(propName, P.between(propValue))
//                break
//            case "within":
//                return edgeOrVertex.has(propName, P.within(propValue))
//                break
//            case "without":
//                return edgeOrVertex.has(propName, P.without(propValue))
//                break
//            default:
//                return edgeOrVertex.has(propName, P.eq(propValue))
//                break
//        }
//    }
}
