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

import org.janusgraph.core.JanusGraphTransaction
import org.janusgraph.core.JanusGraphVertex
import org.janusgraph.graphdb.database.StandardJanusGraph
import org.janusgraph.graphdb.database.management.ManagementSystem
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx
import org.janusgraph.graphdb.vertices.StandardVertex
import org.moqui.impl.entity.EntityDatasourceFactoryImpl
import org.janusgraph.core.schema.JanusGraphManagement
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal

import org.apache.tinkerpop.gremlin.structure.Graph
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.apache.tinkerpop.gremlin.structure.VertexProperty
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph
import org.apache.tinkerpop.gremlin.driver.Client
import org.apache.tinkerpop.gremlin.driver.ResultSet
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor

import java.sql.Timestamp
import java.util.Date
import java.sql.Connection
import java.text.SimpleDateFormat
import org.apache.commons.collections.set.ListOrderedSet

import org.moqui.entity.EntityException
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityFacadeImpl
import org.moqui.impl.entity.EntityValueBase
import org.moqui.impl.entity.EntityValueImpl
import org.moqui.entity.EntityValue
import org.moqui.entity.EntityFind
import org.moqui.impl.entity.FieldInfo
import org.moqui.addons.janusgraph.JanusGraphDatasourceFactory
import org.moqui.addons.janusgraph.JanusGraphEntityFind

import org.janusgraph.core.JanusGraph

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.moqui.util.MNode

import javax.script.Bindings;
import javax.script.SimpleBindings;

class JanusGraphEntityValue extends EntityValueBase {
    protected final static Logger logger = LoggerFactory.getLogger(JanusGraphEntityValue.class)
    //protected long id
    protected JanusGraphDatasourceFactory ddf
    protected org.apache.tinkerpop.gremlin.structure.Vertex vertex
    //protected JanusGraphEntityConditionFactoryImpl conditionFactory

    JanusGraphEntityValue(EntityDefinition ed, EntityFacadeImpl efip, JanusGraphDatasourceFactory ddf) {
        super(ed, efip)
        //this.conditionFactory = new JanusGraphEntityConditionFactoryImpl(efip)
        this.ddf = ddf
        return
    }

    JanusGraphEntityValue(EntityDefinition ed, EntityFacadeImpl efip, JanusGraphDatasourceFactory ddf, Map valMap) {
        super(ed, efip)
        this.ddf = ddf
        return
    }


    JanusGraphEntityValue(EntityDefinition ed, EntityFacadeImpl efip, org.apache.tinkerpop.gremlin.structure.Vertex v) {
        super(ed, efip)
        setVertex(v)
        return
    }

    Graph getDatabase() {
        JanusGraphDatasourceFactory edfi = getDatasource()
        Graph janusGraph = edfi.getDatabase()
        return janusGraph
    }

    JanusGraphDatasourceFactory getDatasource () {
        EntityFacadeImpl efi = getEntityFacadeImpl()
        EntityDefinition ed = getEntityDefinition()
        JanusGraphDatasourceFactory edfi = efi.getDatasourceFactory(ed.getEntityGroupName()) as JanusGraphDatasourceFactory
        return edfi
    }

    org.apache.tinkerpop.gremlin.driver.Client getClient () {
        JanusGraphDatasourceFactory edfi = getDatasource()
        org.apache.tinkerpop.gremlin.driver.Client client = edfi.getClient()
        return client
    }

    GraphTraversalSource getTraversalSource () {
        JanusGraphDatasourceFactory edfi = getDatasource()
        GraphTraversalSource g = edfi.getTraversalSource()
        return g
    }

    public EntityValue create( ) {
        this.create(null)
    }

    public EntityValue create( org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource passedTraversalSource ) {

        org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource g = passedTraversalSource
        org.apache.tinkerpop.gremlin.structure.Vertex v, v2

        GraphTraversal gts
        if (!g) {
            g = getTraversalSource()
        }
        logger.info("in JGEntityValue::create, g: ${g}")
        try {
            EntityDefinition ed = getEntityDefinition()
            String labelName = ed.getEntityNode().attribute("entity-name")
            logger.info("in JGEntityValue::create, labelName: ${labelName}")
            Map <String,Object> vertexProperties = getValueMap()
            logger.info("in JGEntityValue::create, vertexProperties: ${vertexProperties}")
            v2 = JanusGraphUtils.storeVertex(null, labelName, vertexProperties, g, null)
            logger.info("in JGEntityValue::create, v2: ${v2}")
            setVertex(v2)
        } catch (Exception e) {
            logger.info("in JanusGraphEntityValue, exception: ${e.getMessage()}")
        }
        if (!passedTraversalSource) {
            g.tx().commit()
            g.close()
        }
        return this
    }

    public EntityValue update( ) {
        this.update(null)
    }

    public EntityValue update( org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource passedTraversalSource ) {

        org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource g = passedTraversalSource
        if (!g) {
            g = getTraversalSource()
        }

        org.apache.tinkerpop.gremlin.structure.Vertex v, v2

        try {
            EntityDefinition ed = getEntityDefinition()
            String labelName = ed.getEntityNode().attribute("entity-name")
            logger.info("in JGEntityValue::update, labelName: ${labelName}")
            Map <String,Object> vertexProperties = getValueMap()
            logger.info("in JGEntityValue::update, vertexProperties: ${vertexProperties}")
            v = this.getVertex()
            logger.info("in JGEntityValue::update, v: ${v}")
            v2 = JanusGraphUtils.storeVertex(v, labelName, vertexProperties, g, null)
            logger.info("in JGEntityValue::update, v2: ${v2}, emailAddress: ${v.property('emailAddress').value()}")
        } catch (Exception e) {
            String msg = e.getMessage()
            logger.info("in TestCRUD, update_PartyContactInfo, exception: ${msg}")
        }
        if (!passedTraversalSource) {
            g.tx().commit()
            g.close()
        }
        return this
    }

    public EntityValue delete( org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource passedTraversalSource ) {
        org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource g = passedTraversalSource
        if (!g) {
            g = getTraversalSource()
        }

        EntityDefinition ed = getEntityDefinition()
        java.util.Date sameDate = new java.util.Date(new Timestamp(System.currentTimeMillis()).getTime())
        long sameTime = sameDate.getTime()
        Vertex v = this.getVertex()
        Object thisId = v.id()
        if (v) {
            g.V(thisId).next().remove()
            this.setVertex(null)
        }
        if (!passedTraversalSource) {
            g.tx().commit()
            g.close()
        }
    }

    Object getId() {
        return this.getVertex().id()
    }

    void testFunction() {
        return;
    }

    EntityValue cloneValue() {
        // FIXME
        return this
    }

    EntityValue cloneDbValue(boolean b) {
        // FIXME
        return this
    }

    public void createExtended(FieldInfo[] fieldInfoArray, Connection con) {return }
    public void updateExtended(FieldInfo[] pkFieldArray, FieldInfo[] nonPkFieldArray, Connection con) {return }
    public void deleteExtended(Connection con) {return }
    public boolean refreshExtended() { return null}

    HashMap<String, Object> getValueMap() {
        HashMap<String, Object> newValueMap = new HashMap()
        HashMap<String, Object> parentValueMap = super.getValueMap()
        logger.info("parentValueMap: ${parentValueMap}")
        parentValueMap.each{k,v ->
            if (v instanceof Timestamp) {
                logger.info("${k} is Timestamp")
                newValueMap[k] = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(v)
            } else {
                if (v) {
                    newValueMap[k] = v
                }
            }
        }
        logger.info("newValueMap: ${newValueMap}")
        return newValueMap
    }

    Object getDataValue(fieldName) {

        Object retVal
        String labelString, fieldType
        EntityDefinition ed = getEntityDefinition()
        MNode fieldNode = ed.getFieldNode(fieldName)
        fieldType = fieldNode.attribute("type")
        switch(fieldType) {
            case "id":
            case "id-long":
            case "text-short":
            case "text-medium":
            case "text-long":
            case "text-very-long":
            case "text-indicator":
                retVal = getString(fieldName)
                break
            case "number-integer":
            case "number-decimal":
            case "number-float":
            case "currency-amount":
            case "currency-precise":
                retVal = getDouble(fieldName)
                break
            case "date":
            case "time":
            case "date-time":
                //retVal = new java.util.Date(this.get(fieldName).getTime())
                retVal = get(fieldName)
                break
            default:
                retVal = get(fieldName)
        }
        return retVal
    }

    EntityValue fillFromVertex(v) {
        return this
    }

    EntityValue setAll(Map <String,Object> fieldMap) {
        logger.info("in JGEntityValue::setAll, fieldMap: ${fieldMap}")
        //EntityValue entityValue = super.setAll(fieldMap)
        fieldMap.each{fieldName, val ->
            putNoCheck(fieldName, val)
        }
        logger.info("in JGEntityValue::setAll, valueMap: ${getValueMap()}")
        return this
    }

    public Object put(final String name, Object value) {
        //if (!getEntityDefinition().isField(name)) throw new EntityException("The field name " + name + " is not valid for entity " + entityName);
        return putNoCheck(name, value);
    }

    void set(fieldName, value) {
        super.set(fieldName, value)
    }

    void setProperty(fieldName, value) {
        if (this.vertex) {
            EntityDefinition ed = this.getEntityDefinition()
            if (ed.isField(fieldName)) {
                this.vertex.property(name, value)
            }
        }
    }

    org.apache.tinkerpop.gremlin.structure.Vertex getVertex() {
        return this.vertex
    }

    void setVertex(org.apache.tinkerpop.gremlin.structure.Vertex v) {
        this.vertex = v
        if (this.vertex) {
            Iterator <Vertex> propIter = v.properties()
            org.apache.tinkerpop.gremlin.structure.VertexProperty vertexProperty
            EntityDefinition ed = this.getEntityDefinition()
            while(propIter.hasNext()) {
                vertexProperty = propIter.next()
                if (ed.isField(vertexProperty.key())) {
                    this.set(vertexProperty.key(), vertexProperty.value())
                }
            }
        }
        return
    }
}
