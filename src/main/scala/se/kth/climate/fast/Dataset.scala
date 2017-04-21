package se.kth.climate.fast

import org.apache.spark.rdd.RDD
import se.kth.climate.fast.data._
//import se.kth.climate.fast.common._

//trait Dataset[Dimensions, Variables] {
//  def raw: Map[String, RDD[VariableGroup]];
//  def vars: Variables;
//}
//
//class LocalDataset[Dimensions, Variables](val dims: Dimensions, val vars: Variables) extends Dataset[Dimensions, Variables] {
//  override def raw: Map[String, RDD[VariableGroup]] = ???;
//  def on[IndexType: Index, DataType](v: Variable[IndexType, DataType])
//}
//
//object Dataset {
//  def scalar[T](t: T) = new LocalDataset(Const, Constant(t));
//}
//
//trait Selection[IndexType <: Product, DataType] {
//  def map[IndexOut <: Product: Index, DataOut, Dimensions, Variables](f: (Index[IndexType], DataType) => (Index[IndexOut], DataOut))(implicit ev: Index[IndexType]): Dataset[Dimensions, Variables];
//}
//
//class SingleVariableSelection[IndexType <: Product, DataType, Dimensions, Variables](val v: Variable[IndexType, DataType], val ds: Dataset[Dimensions, Variables]) extends Selection[IndexType, DataType] {
//  override def map[IndexOut <: Product: Index, DataOut, DimensionsOut, VariablesOut](f: (Index[IndexType], DataType) => (Index[IndexOut], DataOut))(implicit ev: Index[IndexType]): Dataset[Dimensions, Variables] = {
//
//  }
//}
