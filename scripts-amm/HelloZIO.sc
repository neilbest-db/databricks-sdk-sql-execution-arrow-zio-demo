import $ivy.`dev.zio::zio:2.1.9`
import $ivy.`dev.zio::zio-http:3.0.1`



import zio._

@main
def main() = {

  val myAppLogic = for {
    _ <- Console.printLine("Hello! What is your name?")
    n <- Console.readLine
    _ <- Console.printLine("Hello, " + n + ", good to meet you!")
  } yield ()

  Unsafe.unsafe { implicit unsafe =>
      zio.Runtime.default.unsafe.run(
        myAppLogic
      ).getOrThrowFiberFailure()
  }
}
