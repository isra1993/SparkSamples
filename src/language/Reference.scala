package language

// Object = class with static methods, only one on execution (Singleton)
// An object is an execution input
//    - with main method
//    - App subtype
object Reference {
  
  def main(args : Array[String]) : Unit = {
    
    println("Message from Scala")
    
    var string = "String"
    var otherString : String = "Other string"
    
    string = "New string"
    
    val integer = 10
    val anotherInteger : Int = 20
    
    printf("String: %s - Integer: %d%n", string, integer)
    
    // Code block
    // Are data types
    // Returns last query
    println("Concat " + {
      var counter = 20
      counter += 1
      counter
    })
    
    //Functions
    // Are data types
    // they can be created on any way
    var sum = (a : Double, b : Double) => a + b
    
    def multiply(a : Double, b : Double) : Double = a * b
    
    // Functions with more than one parameters list
    def sumAndMultiply(a : Double,b : Double)(k : Double) = {
      (a + b) * k
    }
    
    println("SumAndMultiply: " + sumAndMultiply(10, 20)(10))
    
    // Partial function
    val partialMultiply = sumAndMultiply(20, 20) _
    println("PartialMultiply: " + partialMultiply(100))
    
    // Functions with default values
    def signinUser(
        name : String = "testuser",
        pass : String = "testpass") : Boolean = {
      printf("User sign in: %s - %s%n", name, pass)
      true
    }
    
    signinUser()
    
    signinUser("John")
    
    signinUser(pass = "one", name = "userone")
    
    // High-order functions
    // - that functions receive function as parameter or returns one or more function
    def executeOperation(
        data : List[Int],
        operation : (Int, Int) => Int) : Int = {
      data.reduce(operation)
    }
    
    def sumTwoIntegers(a : Int, b : Int) : Int = a + b
    
    val result1 = executeOperation(List(10, 20, 30), sumTwoIntegers)
    
    val result2 = executeOperation(List(10, 20, 30), (a, b) => a + b)
    
    val result3 = executeOperation(List(10, 20, 30), _ + _)
    
    printf("Result 1: %d, Result 2: %d, Result 3: %d%n", result1, result2, result3)
    
    // Tuples
    var coordinates = (2.2, 2.4, 2.6)
    var coordinates2 = Tuple3[Double, Double, Double](2.2, 2.4, 2.6)
    
    // Accessing tuples' elements
    printf("Lat: %f, Lon: %f, Alt: %f%n", coordinates._1, coordinates._2, coordinates._3)
    
    var (lat, lon, alt) = coordinates
    printf("Lat: %f, Lon: %f, Alt: %f%n", lat, lon, alt)
  }
}