package se.kth.climate.fast.data

//import se.kth.climate.fast.metadata.{ Var }
//
////trait Variable[IndexType <: Product, DataType] {
////
////  def map[IndexOut <: Product: Index, DataOut](f: (Index[IndexType], DataType) => (Index[IndexOut], DataOut))(implicit ev: Index[IndexType]): Variable[IndexOut, DataOut];
////}
////
////trait Variables[VTS <: Product] {
////
////}
////
////trait VariableGroup {
////
////}
////
////case class Constant[T](value: T) extends Variable[ConstT, T] {
////  override def map[IndexOut <: Product: Index, DataOut](f: (Index[ConstT], T) => (Index[IndexOut], DataOut))(implicit ev: Index[ConstT]): Variable[IndexOut, DataOut] = {
////    val newval = f(Const, value);
////    newval match {
////      case (Const, v) => Constant(newval._2)
////      case _          => ???
////    }
////  }
////}
//
//trait VariableAssignment[IT, DT]
//
//class ConstantAssignment[IT, DT](val variable: Var[IT, DT], val value: DT) extends VariableAssignment[IT, DT] {
//
//}
