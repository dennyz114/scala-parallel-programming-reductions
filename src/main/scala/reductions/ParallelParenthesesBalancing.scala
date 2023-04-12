package reductions

import scala.annotation.*
import org.scalameter.*

object ParallelParenthesesBalancingRunner:

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns := 40,
    Key.exec.maxWarmupRuns := 80,
    Key.exec.benchRuns := 120,
    Key.verbose := false
  ) withWarmer(Warmer.Default())

  def main(args: Array[String]): Unit =
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface:

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean =
    @tailrec
    def parenthesisBalanced(index: Int, openedCount: Int): Boolean = {
      if index == chars.length then openedCount == 0
      else chars(index) match {
        case '(' => parenthesisBalanced(index + 1, openedCount + 1)
        case ')' => if openedCount == 0 then false else parenthesisBalanced(index + 1, openedCount - 1)
        case _ => parenthesisBalanced(index + 1, openedCount)
      }
    }
    parenthesisBalanced(0, 0)

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean =

    def traverse(idx: Int, until: Int, closingCount: Int, openingCount: Int) : (Int, Int) = {
      var overClose = 0
      var opening = 0
      var index = idx
      while (index < until) {
        chars(idx) match {
          case '(' => opening += 1
          case ')' => if opening > 0 then opening-=1 else overClose+=1
          case _ =>
        }
        index+=1
      }
      (overClose, opening)
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {
      if until - from <= threshold then
        traverse(from, until, 0, 0)
      else
        val middle = from + (until - from) / 2
        val ((firstOverClose, firstOpening), (secondOverClose, secondOpening)) = parallel(reduce(from, middle), reduce(middle, until))
        if firstOpening > secondOverClose then (firstOverClose, firstOpening - secondOverClose + secondOpening)
        else (firstOverClose + secondOverClose - firstOpening, secondOpening)

    }

    reduce(0, chars.length) == (0, 0)

  // For those who want more:
  // Prove that your reduction operator is associative!

