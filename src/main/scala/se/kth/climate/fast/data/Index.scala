package se.kth.climate.fast.data

trait Dimensions[T <: Product] {

}

trait Dimension {

}

trait Index[T <: Product] {

}

trait Shape extends Product {

}

abstract class ConstT extends Product with Index[ConstT] {
  override def productArity: Int = 0;
  override def productElement(n: Int): Any = None;
  override def canEqual(other: Any) = other.isInstanceOf[ConstT];
}

object Const extends ConstT;