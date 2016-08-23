/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.spark.h2o.converters


import java.lang.reflect.Constructor
import language.postfixOps
import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import water.fvec.Frame

import scala.collection.immutable.IndexedSeq
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Convert H2OFrame into an RDD (lazily).
  *
  * @param frame  an instance of H2O frame
  * @param colNames names of columns
  * @param sc  an instance of Spark context
  * @tparam A  type for resulting RDD
  * @tparam T  specific type of H2O frame
  */
private[spark]
class H2ORDD[A <: Product: TypeTag: ClassTag, T <: Frame] private(@transient val frame: T,
                                                                  val colNames: Array[String])
                                                                 (@transient sc: SparkContext)
  extends RDD[A](sc, Nil) with H2ORDDLike[T] {

  // Get column names before building an RDD
  def this(@transient fr : T)
          (@transient sc: SparkContext) = this(fr, ReflectionUtils.names[A])(sc)

  // Check that H2OFrame & given Scala type are compatible
  if (colNames.length > 1) {
    colNames.foreach { name =>
      if (frame.find(name) == -1) {
        throw new IllegalArgumentException("Scala type has field " + name +
          " but H2OFrame does not have a matching column; has " + frame.names().mkString(","))
      }
    }
  }

  val types = ReflectionUtils.types[A](colNames)
  override val isExternalBackend = H2OConf(sc).runsInExternalClusterMode

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    new H2ORDDIterator(frameKeyName, split.index)
  }

  private val columnTypeNames = ReflectionUtils.typeNames[A](colNames)

  private val jc = implicitly[ClassManifest[A]].runtimeClass

  private type Deserializer = ReadConverterContext => Int => Any

  private val DefaultDeserializer: Deserializer = _ => _ => null

  private val plainExtractors: Map[String, Deserializer] = Map(
    "Boolean" -> ((source: ReadConverterContext) => (col: Int) => source.getBoolean(col)),
    "Byte"    -> ((source: ReadConverterContext) => (col: Int) => source.getByte(col)),
    "Double"  -> ((source: ReadConverterContext) => (col: Int) => source.getDouble(col)),
    "Float"   -> ((source: ReadConverterContext) => (col: Int) => source.getFloat(col)),
    "Integer" -> ((source: ReadConverterContext) => (col: Int) => source.getInt(col).asInstanceOf[Int]),
    "Long"    -> ((source: ReadConverterContext) => (col: Int) => source.getLong(col)),
    "Short"   -> ((source: ReadConverterContext) => (col: Int) => source.getShort(col).asInstanceOf[Short]),
    "String"  -> ((source: ReadConverterContext) => (col: Int) => source.getString(col)))

  private def returnOption[X](op: ReadConverterContext => Int => X) = (source: ReadConverterContext) => (col: Int) => opt(op(source)(col))

  type TypeName = String

  private val allExtractors: Map[TypeName, Deserializer] = plainExtractors ++
    (plainExtractors map {case (key, op) => s"Option[$key]" -> returnOption(op)})

  private val extractorsMap: Map[TypeName, Deserializer] = allExtractors withDefaultValue DefaultDeserializer

  val extractors = extractorsMap compose columnTypeNames

  private def opt[X](op: => Any): Option[X] = try {
    Option(op.asInstanceOf[X])
  } catch {
    case ex: Exception => None
  }

  class H2ORDDIterator(val keyName: String, val partIndex: Int) extends H2OChunkIterator[A] {
    // maps data columns to product components
    val columnMapping: Map[Int, Int] =
      if (columnTypeNames.size == 1) Map(0->0) else multicolumnMapping

    def multicolumnMapping: Map[Int, Int] = {
      try {
        val mappings = for {
          i <- columnTypeNames.indices
          name = colNames(i)
          j = fr.names().indexOf(name)
        } yield (i, j)

        val bads = mappings collect { case (i, j) if j < 0 => {
          if (i < colNames.length) colNames(i) else s"Unknown index $i (column of type ${columnTypeNames(i)}"
        }
        }

        if (bads.nonEmpty) {
          throw new scala.IllegalArgumentException(s"Missing columns: ${bads mkString ","}")
        }

        mappings.toMap
      }
    }

    private def cell(i: Int) = {
      val j = columnMapping(i)
      val ex = extractors(i)(converterCtx)
      val data = ex(j).asInstanceOf[Object]
//      println(s"@$i/$j = $data")
      data
    }

    def extractRow: Option[Array[AnyRef]] = {
      val rowOpt = opt {
        val objects: IndexedSeq[Object] = columnTypeNames.indices map cell
        val row = objects toArray

//        println(s"${converterCtx.rowIdx} -> ${res mkString ":"}")
        row
      }
      converterCtx.increaseRowIdx()
      rowOpt
    }

    private var hd: Option[A] = None
    private var total = 0

    override def hasNext = {
      while (hd.isEmpty && super.hasNext) {
        hd = readOne()
        total += 1
      }
      hd.isDefined
    }

    def next(): A = {
      if (hasNext) {
        val a = hd.get
        hd = None
        a
      } else {
        throw new NoSuchElementException(s"No more elements in this iterator: found $total  out of ${converterCtx.numRows}")
            }
      }

      private def readOne(): Option[A] = {
            val dataOpt = extractRow

            val res: Seq[A] = for {
            builder <- builders
            data <- dataOpt
            instance <- builder(data)
            } yield instance

            res.toList match {
            case Nil => None
            case unique :: Nil => Option(unique)
            case one :: two :: more => throw new scala.IllegalArgumentException(
            s"found more than une $jc constructor for given args - can't choose")
          }
        }
  }


  lazy val constructors: Seq[Constructor[_]] = {

    val cs = jc.getConstructors
    val found = cs.collect {
      case c if c.getParameterTypes.length == colNames.length => c
    }

    if (found.isEmpty) throw new scala.IllegalArgumentException(
      s"Constructor must take exactly ${colNames.length} args")

    found
  }

  case class Builder(c:  Constructor[_]) {
    def apply(data: Array[AnyRef]): Option[A] = {
      opt(c.newInstance(data:_*).asInstanceOf[A])
    }
  }

  private lazy val builders = constructors map Builder

  if (false) {

    val iterator = new H2OChunkIterator[A] {

      val jc = implicitly[ClassTag[A]].runtimeClass
      val cs = jc.getConstructors
      val ccr = cs.collectFirst({
                case c if c.getParameterTypes.length == colNames.length => c
              })
        .getOrElse({
            throw new IllegalArgumentException(
                  s"Constructor must take exactly ${colNames.length} args")
      })

      val expectedTypes: Option[Array[Byte]] = ConverterUtils.prepareExpectedTypes(isExternalBackend, types)
      override val keyName = frameKeyName
      override val partIndex = 42//split.index

      def next(): A = {
        val data = new Array[Option[Any]](ncols)
        // FIXME: this is not perfect since ncols does not need to match number of names
        (0 until ncols).foreach{ idx =>
          val value = if (converterCtx.isNA(idx)) None
          else types(idx) match {
            case q if q == classOf[Integer]           => Some(converterCtx.getInt(idx))
            case q if q == classOf[java.lang.Long]    => Some(converterCtx.getLong(idx))
            case q if q == classOf[java.lang.Double]  => Some(converterCtx.getDouble(idx))
            case q if q == classOf[java.lang.Float]   => Some(converterCtx.getFloat(idx))
            case q if q == classOf[java.lang.Boolean] => Some(converterCtx.getBoolean(idx))
            case q if q == classOf[String] => Option(converterCtx.getString(idx))
            case _ => None
          }
          data(idx) = value
        }

        converterCtx.increaseRowIdx()
        // Create instance for the extracted row
        ccr.newInstance(data:_*).asInstanceOf[A]
      }
    }

    ConverterUtils.getIterator[A](isExternalBackend, iterator)
  }
}
