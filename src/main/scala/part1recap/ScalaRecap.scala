package part1recap

import org.apache.zookeeper.Environment.list

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaRecap extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expressions
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instruction vs expressions
  // instruction are using in c++,java,c
  // expressions are function programming like using scala

  val theUnit = println("Hello,Scala") // Unit = "no Meaningful value" = void in other languages

  // functions
  def myFunciton(x: Int) = 42

  // OOP (single class inheritance)
  class Animal

  class Cat extends Animal

  trait Carnivore {
    def eat(animal: Animal): Unit
  }

  class Crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch!")
  }

  // singleton pattern
  object MySingleton

  // companions --> if we have class and object with same name in same file it called companions
  object Carnivore

  // generics
  trait MyList[+A]

  // method notation (infix,postfix,prefix) but mostly used infix --> all operators are method in scala
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programming (func 1 to 22)
  val incrementer: Function1[Int, Int] = new Function[Int, Int] {
    override def apply(x: Int): Int = 42
  }

  // Anonymous Functions/ Lambda
  val incrementer1: Int => Int = x => x + 1

  //val incremented = incrementer(42)

  // map, flatMap, filter
  val processedList = List(1, 2, 3).map(incrementer)

  // Pattern Matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "first"
    case 2 => "second"
    case _ => "unknown"
  }

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case e: NullPointerException => "some returned value"
    case _ => "something else"
  }

  // Future

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    // some expensive computation, runs on another thread
    42
  }

  aFuture.onComplete {
    case Success(meaningOfList) => println(s"I've found $meaningOfList")
    case Failure(ex) => println(s"I've failed : $ex")
  }

  // Partial Functions
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 999


    /*val aPartialFunction = (x : Int) => x match {
      case 1 => 43
      case 8 => 56
      case _ => 999
    */
    // same as anonymous functions
  }

  // Implicits

  // auto-Injection by the compiler
  def methodWithImplicitArgument(implicit x: Int) = x + 43

  implicit val imlicitInt = 67
  val implicitCall = methodWithImplicitArgument

  // implicit conversions - implicits defs

  // case class -> light weight of data structure and bunch of utility methods already create by compiler (toString(), equals(), hashcode(), apply(), )
  case class Person(name: String) {

    def greet = println(s"Hi, My name is $name")
  }

  implicit def fromStringToPerson(name: String) = Person(name)

  "bob".greet // compiler automatically does for us -->  fromStringToPerson("bob").greet

  // Implicit conversion - Implicit Classes
  implicit class Dog(name: String) {
    def bark = println("Bark!")
  }

  "Lassie".bark // "lassie" will convert into Dog(name) type and then we call then the method

  /*
      - Local Scope   -->  explicitly define implicit value --> implicit val implicitInt = 67
      - imported Scope --> importing from library -->  import scala.concurrent.ExecutionContext.Implicits.global (in this program we use for Future)
      - companion objects of the types involved in the method call and sorted is implicit Ordering  --> List(-1, -2, -3).sorted and compiler look for companion object of list or Type[]
  */



}












































