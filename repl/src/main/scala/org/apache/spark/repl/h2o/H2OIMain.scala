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

package org.apache.spark.repl.h2o


import java.io.File
import java.lang.reflect.Field

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.repl.{Main, SparkIMain}
import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}


import scala.collection.mutable
import scala.reflect.internal.util.BatchSourceFile
import scala.tools.nsc.interpreter.AbstractOrMissingHandler
import scala.tools.nsc.{Global, Settings, io}

/**
  * SparkIMain which allows parallel interpreters to coexist. It adds each class to package which specifies the interpreter
  * where this class was declared
  */
private[repl] class H2OIMain private(initialSettings: Settings,
                                     interpreterWriter: IntpResponseWriter,
                                     val sessionID: Int,
                                     propagateExceptions: Boolean = false) extends{
  override private[repl] val outputDir: File = H2OIMain.classOutputDirectory
} with SparkIMain(initialSettings, interpreterWriter, propagateExceptions){

  setupClassNames()

  /**
    * This method has to be overridden because it has to call "overridden" method _initialize
    */
  @DeveloperApi
  override def initializeSynchronous(): Unit = {
    if (!getPrivateFieldValue("_initializeComplete").asInstanceOf[Boolean]) {
      _initialize()
      assert(global != null, global)
    }
  }

  private def _initialize() = {
    try {
      // todo. if this crashes, REPL will hang
      val _compiler: Global = getPrivateFieldValue("_compiler").asInstanceOf[Global]
      new _compiler.Run().compileSources(_initSources)
      setPrivateFieldValue("_initializeComplete", true)
      true
    }
    catch AbstractOrMissingHandler()

  }

  /**
    * This is needed to "override" in order to ensure each interpreter lives in its own package
    * @return
    */
  private def _initSources = List(new BatchSourceFile("<init>", "package intp_id_" + sessionID + " \n class $repl_$init { }"))

  /**
    * Ensures that before each class is added special prefix identifying the interpreter
    * where this class was declared
    */
  private def setupClassNames(): Unit ={
    val ru = scala.reflect.runtime.universe
    val mirror = ru.runtimeMirror(getClass.getClassLoader)

    val fixedSessionNamesSymbol = ru.typeOf[FixedSessionNames.type].termSymbol.asModule

    val moduleMirror = mirror.reflect(this).reflectModule(fixedSessionNamesSymbol)
    val instanceMirror = mirror.reflect(moduleMirror.instance)

    val lineNameTerm = ru.typeOf[FixedSessionNames.type].declaration(ru.newTermName("lineName")).asTerm.accessed.asTerm
    val fieldMirror = instanceMirror.reflectField(lineNameTerm)

    val previousValue = fieldMirror.get.asInstanceOf[String]
    fieldMirror.set("intp_id_" + sessionID + "." + previousValue)
  }

  override private[repl] def initialize(postInitSignal: => Unit): Unit = {
    synchronized {
      if (getPrivateFieldValue("_isInitialized") == null) {
        setPrivateFieldValue("_isInitialized", io.spawn {
          try _initialize()
          finally postInitSignal
        })
      }
    }
  }


  private def getPrivateField(fieldName: String): Field = {
    val field = this.getClass.getSuperclass.getDeclaredField(fieldName)
    field.setAccessible(true)
    field
  }

  private def getPrivateFieldValue(fieldName: String): AnyRef = {
    getPrivateField(fieldName).get(this)
  }

  private def setPrivateFieldValue(fieldName: String, newValue: Any): Field = {
    val field = getPrivateField(fieldName)
    field.set(this, newValue)
    field
  }
}

object H2OIMain {


  val existingInterpreters = mutable.HashMap.empty[Int, H2OIMain]
  private var interpreterClassloader: InterpreterClassLoader = _
  private var _initialized = false

  private def setClassLoaderToSerializers(classLoader: ClassLoader): Unit = {
    SparkEnv.get.serializer.setDefaultClassLoader(classLoader)
    SparkEnv.get.closureSerializer.setDefaultClassLoader(classLoader)
  }

  private def initialize(sc: SparkContext): Unit = {

    if (Main.interp != null) {
      //application has been started using SparkSubmit, reuse the classloader
      interpreterClassloader = new InterpreterClassLoader(Main.interp.intp.classLoader)
    } else {
      //application hasn't been started using SparkSubmit
      interpreterClassloader = new InterpreterClassLoader()
    }
    setClassLoaderToSerializers(interpreterClassloader)
  }

  def getInterpreterClassloader: InterpreterClassLoader = {
    interpreterClassloader
  }


  def createInterpreter(sc: SparkContext, settings: Settings, interpreterWriter: IntpResponseWriter, sessionId: Int): H2OIMain = synchronized {
    if(!_initialized){
      initialize(sc)
      _initialized = true
    }
    existingInterpreters += (sessionId -> new H2OIMain(settings, interpreterWriter, sessionId, false))
    existingInterpreters(sessionId)
  }

  private lazy val _classOutputDirectory = {
    if (org.apache.spark.repl.Main.interp != null) {
      // Application was started using shell, we can reuse this directory
      org.apache.spark.repl.Main.interp.intp.getClassOutputDirectory
    } else {
      // REPL hasn't been started yet, create new directory
      val conf = new SparkConf()
      val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
      val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")
      outputDir
    }
  }

  def classOutputDirectory = _classOutputDirectory
}
