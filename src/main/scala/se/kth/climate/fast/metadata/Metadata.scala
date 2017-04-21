package se.kth.climate.fast.metadata

//import se.kth.climate.fast.data.VariableAssignment

trait Metadata { //[T <: Product] {
  //def *(): T;

  //  def d[IT](size: Long): Dim[IT] = new Dim[IT](size);
  //  def v[VT](): PreVar[VT] = new PreVar[VT]();
}

//class Var[IT, DT](dims: IT, name: Option[String]) {
//  def :=(v: DT): VariableAssignment[IT, DT]
//}
//
//abstract class ConstDimT extends Product {
//  override def productArity: Int = 0;
//  override def productElement(n: Int): Any = None;
//  //override def canEqual(other: Any) = other.isInstanceOf[ConstDim];
//}
//
//object ConstDim extends ConstDimT
//
//case class Const[DT](name: Option[String]) extends Var[ConstDimT, DT](ConstDim, name) {
//  def :=(v: DT): VariableAssignment[ConstDim, DT] = ConstantAssignment(this, v);
//}
//
//class Dim[IT](val size: Long)
//
//class PreVar[VT]() {
//  def indexedBy[IT](dims: IT): Var[IT, VT] = new Var[IT, VT](dims, None);
//}
