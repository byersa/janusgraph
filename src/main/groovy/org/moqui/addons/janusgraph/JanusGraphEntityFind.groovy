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

        long startTime = System.currentTimeMillis()
        List retList = null
        StringBuilder sb = StringBuilder.newInstance()
        if(fromVertexId) {
            sb << "g.V('${fromVertexId}')"
        } else {
            sb << "g.V()"
        }
        if (vertexProperties && vertexProperties.size()) {
            vertexProperties.each { tuple ->
                sb << applyPredicate(tuple[0], tuple[1], tuple[2])
            }
        }
        sb << ".toList()"
        String gremlin = sb.toString()
        org.apache.tinkerpop.gremlin.driver.ResultSet results = client.submit(gremlin.toString());
        CompletableFuture <org.apache.tinkerpop.gremlin.structure.Vertex> cf = results.all()
        List <Result> resultList = cf.get()
        EntityValue entityValue
        EntityDefinition ed
        org.apache.tinkerpop.gremlin.structure.Vertex vtx
        org.apache.tinkerpop.gremlin.driver.Result result
        if (resultList.size()) {
            result = resultList[0]
            vtx = result.getVertex()
            ed = efi.getEntityDefinition(vtx.label())
            // TODO: if no ed then throw exception
            entityValue = new JanusGraphEntityValue(ed, efi, vtx)
            return entityValue
        } else {
            return null
        }
    }

    /** @see org.moqui.entity.EntityFind#list() */
    List <EntityValue> list() {
        long startTime = System.currentTimeMillis()
        //EntityDefinition ed = this.getEntityDef()
        List retList = null
        //JanusGraphEntityValue entValue = null
        //EntityList entList = new EntityListImpl(this.efi)
        //logger.info("JanusGraphEntityFind.list efi: ${this.efi}")
        StringBuilder sb = StringBuilder.newInstance()
        if(fromVertexId) {
            sb << "g.V('${fromVertexId}')"
        } else {
            sb << "g.V()"
        }
        if(edgeLabel) {
            sb << ".outE('${edgeLabel}')"
        } else {
            sb << ".outE()"
        }
        if (edgeProperties && edgeProperties.size()) {
            edgeProperties.each { tuple ->
                sb << applyPredicate(tuple[0], tuple[1], tuple[2])
            }
        }
        if (vertexLabel) {
            sb << ".inV().hasLabel('${vertexLabel}')"
        } else {
            sb << ".inV()"
        }
        if (vertexProperties && vertexProperties.size()) {
            vertexProperties.each { tuple ->
                sb << applyPredicate(tuple[0], tuple[1], tuple[2])
            }
        }
        sb << ".toList()"
        EntityValue entityValue
        String gremlin = sb.toString()
        org.apache.tinkerpop.gremlin.driver.ResultSet results = client.submit(gremlin.toString())
        CompletableFuture <org.apache.tinkerpop.gremlin.structure.Vertex> cf = results.all()
        List <Result> resultList = cf.get()
        EntityDefinition ed
        org.apache.tinkerpop.gremlin.structure.Vertex vtx
        List <EntityValue> entityValueList = new ArrayList()
        resultList.each {result ->
            vtx = result.getVertex()
            ed = efi.getEntityDefinition(vtx.label())
            // TODO: if no ed then throw exception
            entityValue = new JanusGraphEntityValue(ed, efi, vtx)
            entityValueList << entityValue
        }
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

    String applyPredicate( String propName, String predicateText, Object propValue) {

        String predicateValue
        if (propValue instanceof String) {
            predicateValue = "'${propValue}'"
        } else if (propValue instanceof Float) {
            predicateValue = "${propValue}f"
        } else {
            predicateValue = "${propValue}"
        }

        switch (predicateText) {
            case "eq":
                return ".has('${propName}',  eq(${predicateValue}))"
                break
            case "neq":
                return ".has('${propName}',  neq(${predicateValue}))"
                break
            case "lt":
                return ".has('${propName}',  lt(${predicateValue}))"
                break
            case "lte":
                return ".has('${propName}',  lte(${predicateValue}))"
                break
            case "gt":
                return ".has('${propName}',  gt(${predicateValue}))"
                break
            case "gte":
                return ".has('${propName}',  gte(${predicateValue}))"
                break
            case "inside":
                return ".has('${propName}',  inside(${predicateValue}))"
                break
            case "outside":
                return ".has('${propName}',  outside(${predicateValue}))"
                break
            case "between":
                return ".has('${propName}',  between(${predicateValue}))"
                break
            case "within":
                return ".has('${propName}',  within(${predicateValue}))"
                break
            case "without":
                return ".has('${propName}',  without(${predicateValue}))"
                break
            default:
                return ".has('${propName}',  eq(${predicateValue}))"
                break
        }
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
